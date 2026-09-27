unit class Redis;

has Str $.host = '127.0.0.1';
has Int $.port = 6379;
has Str $.sock; # if sock is defined, use sock
has Str $.encoding = "UTF-8"; # Use this encoding to decode Str
# If True, decode Buf response into Str, except following methods:
#   dump
# which, must return Buf
has Bool $.decode_response = False;
has $.conn is rw;
has $.command-lock = Lock.new;

# Predefined callbacks
my &status_code_reply_cb = { $_ eq "OK" };
my &integer_reply_cb = { $_.Bool };
my &buf_to_float_cb = { $_.decode("ASCII").Real };

my %command_callbacks;
%command_callbacks{"PING"} = { $_ eq "PONG" };
for <<
  "CLIENT KILL" BGSAVE BGREWRITEAOF AUTH QUIT SET MSET PSETEX SETEX MIGRATE
  RENAME RENAMENX RESTORE HMSET SELECT LSET LTRIM FLUSHALL FLUSHDB DISCARD
  MULTI WATCH UNWATCH "SCRIPT FLUSH" SCRIPT KILL
>> -> $c {
    %command_callbacks{$c} := &status_code_reply_cb;
}
for <
  EXISTS SETNX EXPIRE EXPIREAT MOVE PERSIST PEXPIRE PEXPIREAT HSET HEXISTS
  HSETNX SISMEMBER SMOVE
> -> $c {
    %command_callbacks{$c} := &integer_reply_cb;
}
for <INCRBYFLOAT HINCRBYFLOAT ZINCRBY ZSCORE> -> $c {
    %command_callbacks{$c} := &buf_to_float_cb;
}
# TODO so ugly...
# @see hash key is Str in ISO-8859-1 encoding
%command_callbacks<HGETALL> = -> @list --> Hash:D {
    my %h;
    for @list.kv -> $i, $key {
        %h{$key.decode("ISO-8859-1")} := @list[$i + 1] if $i %% 2;
    }
    %h
}

%command_callbacks{"INFO"} := -> $info --> Hash:D {
    my @lines = $info.decode.split("\r\n");
    my %info;
    for @lines -> $l {
        if $l.substr(0, 1) ne "#" {
            my ($key, $value) = $l.split(":");
            %info{$key} = $value;
        }
    }
    %info
}

has %!command_callbacks = %command_callbacks;

method new(Str $server?, Str :$encoding?, Bool :$decode_response?) {
    my %config;
    if $server.defined {
        if $server ~~ m/^([\d+]+ %\.) [':' (\d+)]?$/ {
            %config<host> = $0.Str;
            if $1 {
                %config<port> = $1.Str.Int;
            }
        }
        else {
            %config<sock> = $server;
        }
    }
    %config<encoding>        = $_ with $encoding;
    %config<decode_response> = $_ with $decode_response;

    my $obj = self.bless(|%config);
    $obj.reconnect;
    $obj
}

method reconnect {
    if $.sock.defined {
        die "Sorry, connecting via unix sock is currently unsupported!";
    }
    else {
        $.conn = IO::Socket::INET.new(host => $.host, port => $.port, input-line-separator => "\r\n");
    }
}

multi method encode(Str:D $value --> Buf:D) {
    Buf.new($value.encode(self.encoding));
}

multi method encode(Buf:D $value --> Buf:D) {
    $value
}

# convert to Str, then to Buf
multi method encode($value --> Buf:D) {
    self.encode($value.Str);
}

method !pack_command(*@args --> Buf:D) {
    my $cmd = self.encode('*' ~ @args.elems ~ "\r\n");
    for @args -> $arg {
        my $new = self.encode($arg);
        $cmd ~= '$'.encode;
        $cmd ~= self.encode($new.bytes);
        $cmd ~= "\r\n".encode;
        $cmd ~= $new;
        $cmd ~= "\r\n".encode;
    }

    $cmd
}

method exec_command(Str $command, *@args) {
    $!command-lock.protect: {
        @args.prepend($command.split(" "));
        $.conn.write(self!pack_command(|@args));
        my $remainder = Buf.new;

        self!parse_response(self!read_response($remainder), $command);
    }
}

my sub find-first-line-end(Blob $input --> Int:D) {
    my int $i;
    my $input-bytes = $input.bytes;
    while $i + 2 <= $input-bytes  {
        if $input[$i] == 0x0d && $input[$i+1]==0x0a {
            return $i;
        }
        ++$i;
    }
    $input-bytes
}

method !get-first-line(Blob $buf is copy) {
    $buf = Buf[uint8].new unless $buf.?bytes;
    my $first-line-end = find-first-line-end($buf);
    while $first-line-end >= $buf.bytes {
        $buf ~= $.conn.recv(:bin) // die "failed to get more bytes";
        $first-line-end = find-first-line-end($buf);
    }
    my $first-line = $buf.subbuf(0, $first-line-end);
    my $remainder-length = $buf.bytes - ( $first-line-end + 2 );
    my $remainder = $buf.subbuf($first-line-end + 2, $remainder-length);

    ( $first-line.decode, $remainder );

}

# Returns Str/Int/Buf/Array
method !read_response(Blob:D $remainder is rw) {
    my $first-line;
    ($first-line, $remainder)  = self!get-first-line($remainder);
    my ($flag, $response) = $first-line.substr(0, 1), $first-line.substr(1);
    if $flag !eq any('+', '-', ':', '$', '*') {
        die "Unknown response from redis: { $first-line.encode.gist } \n";
    }
    if $flag eq '+' {
        # single line reply, pass
    }
    elsif $flag eq '-' {
        # on error, throw exception
        die $response;
    }
    elsif $flag eq ':' {
        # int value
        $response = $response.Int;
    }
    elsif $flag eq '$' {
        # bulk response
        my $length = $response.Int;
        if $length == -1 {
            return Nil;
        }
        $response = $remainder;
        while $response.bytes < $length + 2 {
            $response.append: $.conn.recv(:bin);
        }
        $remainder = $response.subbuf($length + 2, *);
        $response = $response.subbuf(0, $length);
        if $response.bytes !eq $length {
            die "Invalid response. Wanted $length got { $response.bytes }";
        }
    }
    elsif $flag eq '*' {
        # multi-bulk response
        my $length = $response.Int;
        if $length == -1 {
            return Nil;
        }
        $response = [];
        for 1..$length {
            $response.push(self!read_response($remainder));
        }
    }

    $response
}

method !decode_response($response) {
    if $response~~ Buf[uint8] {
        $response.decode(self.encoding)
    }
    elsif $response ~~ Array {
        $response.map( { self!decode_response($_) } ).Array
    }
    elsif $response ~~ Hash {
        my %h = Hash.new;
        for $response.pairs {
            %h{.key} = self!decode_response(.value);
        }
        %h
    }
    else {
        $response
    }
}

method !parse_response($response is copy, Str $command) {
    if %!command_callbacks{$command}:exists {
        $response =  %!command_callbacks{$command}($response);
    }
    if self.decode_response and $command !eq any("DUMP") {
        $response = self!decode_response($response);
    }

    $response
}

####### Commands/Connection #######

method auth(Str $password --> Bool:D) {
    self.exec_command("AUTH", $password)
}

method echo($message --> Str:D) {
    self.exec_command("ECHO", $message)
}

# Ping the server.
method ping(--> Bool:D) {
    self.exec_command("PING")
}

# Ask the server to close the connection. The connection is closed as soon as all pending replies have been written to the client.
method quit(--> Bool:D) {
    self.exec_command("QUIT")
}

method select(Int $index --> Bool:D) {
    self.exec_command("SELECT", $index)
}

####### ! Commands/Connection #######

####### Commands/Server #######

method bgrewriteaof(--> Bool:D) {
    self.exec_command("BGREWRITEAOF")
}

method bgsave(--> Bool:D) {
    self.exec_command("BGSAVE")
}

method client_kill(Str:D $ip, Int:D $port --> Bool:D) {
    self.exec_command("CLIENT KILL", $ip, $port)
}

method flushall(--> Bool:D) {
    self.exec_command("FLUSHALL")
}

method flushdb(--> Bool:D) {
    self.exec_command("FLUSHDB")
}

method info(--> Hash:D) {
    self.exec_command("INFO")
}

####### ! Commands/Server #######

###### Commands/Keys #######

method del(*@keys --> Int:D) {
    self.exec_command("DEL", |@keys)
}

method dump(Str:D $key --> Buf:D) {
    self.exec_command("DUMP", $key)
}

method exists(Str:D $key --> Bool:D) {
    self.exec_command("EXISTS", $key)
}

method expire(Str:D $key, Int:D $seconds --> Bool:D) {
    self.exec_command("EXPIRE", $key, $seconds)
}

method expireat(Str:D $key, Int:D $timestamp --> Bool:D) {
    self.exec_command("EXPIREAT", $key, $timestamp)
}

method ttl(Str:D $key --> Int:D) {
    self.exec_command("TTL", $key)
}

method keys(Str $pattern --> List:D) {
    self.exec_command("KEYS", $pattern)
}

method migrate(
  Str:D $host, Int:D $port, Str:D $key, Str:D $destination-db, Int:D $timeout
--> Bool:D) {
    self.exec_command("MIGRATE", $host, $port, $key, $destination-db, $timeout)
}

method move(Str:D $key, Str:D $db --> Bool:D) {
    self.exec_command("MOVE", $key, $db)
}

method object(Str:D $subcommand, *@arguments) {
    self.exec_command("OBJECT", $subcommand, |@arguments)
}

method persist(Str:D $key --> Bool:D) {
    self.exec_command("PERSIST", $key)
}

method pexpire(Str:D $key, Int:D $milliseconds --> Bool:D) {
    self.exec_command("PEXPIRE", $key, $milliseconds)
}

method pexpireat(Str:D $key, Int:D $milliseconds-timestamp --> Bool:D) {
    self.exec_command("PEXPIREAT", $key, $milliseconds-timestamp)
}

method pttl(Str:D $key --> Int:D) {
    self.exec_command("TTL", $key)
}

method randomkey() {
    self.exec_command("RANDOMKEY")
}

method rename(Str:D $key, Str:D $newkey --> Bool:D) {
    self.exec_command("RENAME", $key, $newkey)
}

method renamenx(Str:D $key, Str:D $newkey --> Bool:D) {
    self.exec_command("RENAMENX", $key, $newkey)
}

method restore(
  Str:D $key, Int:D $milliseconds, Buf:D $serialized-value
--> Bool:D) {
    self.exec_command("RESTORE", $key, $milliseconds, $serialized-value)
}

method sort(
  Str:D $key,
  Str:D :$by?,
    Int :$offset?,
    Int :$count?,
        :@get?,
   Bool :$desc = False,
   Bool :$alpha = False,
    Str :$store?
  --> List:D) {
    if ($offset.defined and !$count.defined) or (!$offset.defined and $count.defined) {
        die "`offset` and `count` must both be specified.";
    }
    say $desc;
    # TODO
    []
}

# Returns the string representation of the type of the value stored at key. The
# different types that can be returned are: none, string, list, set, zset and hash.
method type(Str:D $key) {
    self.exec_command("TYPE", $key)
}

###### ! Commands/Keys #######

###### Commands/Strings ######

method append(Str:D $key, $value --> Int:D) {
    self.exec_command("APPEND", $key, $value);
}

method bitcount(Str:D $key, Int $start?, Int $end? --> Int:D) {
    my @args = $key;
    if $start.defined and $end.defined {
        @args.push($start);
        @args.push($end);
    }
    elsif $start.defined or $end.defined {
        die "Both start and end must be specified.";
    }

    self.exec_command("BITCOUNT", |@args)
}

method bitop(Str:D $op, Str:D $key, *@keys) {
    self.exec_command("BITOP", $op, $key, |@keys)
}

method get(Str:D $key) {
    self.exec_command("GET", $key)
}

method set(Str:D $key, $value --> Bool:D) {
    self.exec_command("SET", $key, $value)
}

method setbit(Str:D $key, Int:D $offset, $value --> Int:D) {
    self.exec_command("SETBIT", $key, $offset, $value)
}

method setex(Str:D $key, Int:D $seconds, $value) {
    self.exec_command("SETEX", $key, $seconds, $value)
}

method setnx(Str:D $key, $value --> Bool:D) {
    self.exec_command("SETNX", $key, $value)
}

method setrange(Str:D $key, Int:D $offset, $value --> Int:D) {
    self.exec_command("SETRANGE", $key, $offset, $value)
}

method strlen(Str:D $key --> Int:D) {
    self.exec_command("STRLEN", $key)
}

method getbit(Str:D $key, Int:D $offset --> Int:D) {
    self.exec_command("GETBIT", $key, $offset)
}

method getrange(Str:D $key, Int:D $start, Int:D $end --> Str:D) {
    self.exec_command("GETRANGE", $key, $start, $end)
}

method getset(Str:D $key, $value) {
    self.exec_command("GETSET", $key, $value)
}

method incrbyfloat(Str:D $key, Real $increment --> Real:D) {
    self.exec_command("INCRBYFLOAT", $key, $increment)
}

method mget(*@keys) {
    self.exec_command("MGET", |@keys)
}

# Sets the given keys to their respective values.
# Arguments can be named or positional parameters.
method mset(*@args, *%named) {
    for %named {
        @args.push(.key);
        @args.push(.value);
    }
    self.exec_command("MSET", |@args)
}

method msetnx(*@args, *%named) {
    for %named {
        @args.push(.key);
        @args.push(.value);
    }
    self.exec_command("MSETNX", |@args)
}

method psetex(Str:D $key, Int:D $milliseconds, $value) {
    self.exec_command("PSETEX", $key, $milliseconds, $value)
}

method incr(Str:D $key) {
    self.exec_command("INCR", $key)
}

method incrby(Str:D $key, Int:D $increment) {
    self.exec_command("INCRBY", $key, $increment)
}

method decr(Str:D $key) {
    self.exec_command("DECR", $key)
}

method decrby(Str:D $key, Int:D $increment) {
    self.exec_command("DECRBY", $key, $increment)
}

###### ! Commands/Strings ######

###### Commands/Hashes ######

method hdel(Str:D $key, *@fields --> Int:D) {
    self.exec_command("HDEL", $key, |@fields)
}

method hexists(Str:D $key, $field --> Bool:D) {
    self.exec_command("HEXISTS", $key, $field)
}

method hget(Str:D $key, $field) {
    self.exec_command("HGET", $key, $field)
}

method hgetall(Str:D $key --> Hash:D) {
    self.exec_command("HGETALL", $key)
}

method hincrby(Str:D $key, $field, Int $increment --> Int:D) {
    self.exec_command("HINCRBY", $key, $field, $increment)
}

method hincrbyfloat(Str:D $key, $field, Real $increment --> Real:D) {
    self.exec_command("HINCRBYFLOAT", $key, $field, $increment)
}

method hkeys(Str:D $key --> List:D) {
    self.exec_command("HKEYS", $key)
}

method hlen(Str:D $key --> Int:D) {
    self.exec_command("HLEN", $key)
}

method hmget(Str:D $key, *@fields --> List:D) {
    self.exec_command("HMGET", $key, |@fields)
}

method hmset(Str:D $key, *@args, *%named --> Bool:D) {
    for %named {
        @args.push(.key);
        @args.push(.value);
    }
    self.exec_command("HMSET", $key, |@args)
}

method hset(Str:D $key, $field, $value --> Bool:D) {
    self.exec_command("HSET", $key, $field, $value)
}

method hsetnx(Str:D $key, $field, $value --> Bool:D) {
    self.exec_command("HSETNX", $key, $field, $value)
}

method hvals(Str:D $key --> List:D) {
    self.exec_command("HVALS", $key)
}

###### ! Commands/Hashes ######

###### Commands/Lists ######

method blpop(Int:D $timeout, *@keys) {
    self.exec_command("BLPOP", |@keys, $timeout)
}

method brpop(Int:D $timeout, *@keys) {
    self.exec_command("BRPOP", |@keys, $timeout)
}

method brpoplpush(Str:D $source, Str:D $destination, Int:D $timeout) {
    self.exec_command("BRPOPLPUSH", $source, $destination, $timeout)
}

# Returns the element at index in the list, or nil when index is out of range.
method lindex(Str:D $key, Int:D $index) {
    self.exec_command("LINDEX", $key, $index)
}

method linsert(
  Str:D $key,
  Str:D $where where { $where eq any("BEFORE", "AFTER") },
        $pivot,
        $value
--> Int:D) {
    self.exec_command("LINSERT", $key, $where, $pivot, $value)
}

method llen(Str:D $key --> Int:D) {
    self.exec_command("LLEN", $key)
}

method lpop(Str:D $key) {
    self.exec_command("LPOP", $key)
}

method lpush(Str:D $key, *@values --> Int:D) {
    self.exec_command("LPUSH", $key, |@values)
}

method lpushx(Str:D $key, $value --> Int:D) {
    self.exec_command("LPUSHX", $key, $value)
}

method lrange(Str:D $key, Int:D $start, Int:D $stop --> List:D) {
    self.exec_command("LRANGE", $key, $start, $stop)
}

method lrem(Str:D $key, Int:D $count, $value --> Int:D) {
    self.exec_command("LREM", $key, $count, $value)
}

method lset(Str:D $key, Int:D $index, $value) {
    self.exec_command("LSET", $key, $index, $value)
}

method ltrim(Str:D $key, Int:D $start, Int $stop) {
    self.exec_command("LTRIM", $key, $start, $stop)
}

method rpop(Str:D $key) {
    self.exec_command("RPOP", $key)
}

method rpoplpush(Str:D $source, Str:D $destination --> Str:D) {
    self.exec_command("RPOPLPUSH", $source, $destination)
}

method rpush(Str:D $key, *@values --> Int:D) {
    self.exec_command("RPUSH", $key, |@values)
}

method rpushx(Str:D $key, $value) {
    self.exec_command("RPUSHX", $key, $value)
}

###### ! Commands/Lists ######

###### Commands/Sets #######

method sadd(Str:D $key, *@members --> Int:D) {
    self.exec_command("SADD", $key, |@members)
}

method scard(Str:D $key --> Int:D) {
    self.exec_command("SCARD", $key)
}

method sdiff(*@keys --> List:D) {
    self.exec_command("SDIFF", |@keys)
}

method sdiffstore(Str:D $destination, *@keys --> Int:D) {
    self.exec_command("SDIFFSTORE", $destination, |@keys)
}

method sinter(*@keys --> List:D) {
    self.exec_command("SINTER", |@keys)
}

method sinterstore(Str:D $destination, *@keys --> Int:D) {
    self.exec_command("SINTERSTORE", $destination, |@keys)
}

method sismember(Str:D $key, $member) {
    self.exec_command("SISMEMBER", $key, $member)
}

method smembers(Str:D $key --> List:D) {
    self.exec_command("SMEMBERS", $key)
}

method smove(Str:D $source, Str:D $destination, $member --> Bool:D) {
    self.exec_command("SMOVE", $source, $destination, $member)
}

method spop(Str:D $key) {
    self.exec_command("SPOP", $key)
}

method srandmember(Str:D $key) {
    self.exec_command("SRANDMEMBER", $key)
}

method srem(Str:D $key, *@members --> Int:D) {
    self.exec_command("SREM", $key, |@members)
}

method sunion(*@keys --> List:D) {
    self.exec_command("SUNION", |@keys)
}

method sunionstore(Str:D $destination, *@keys --> Int:D) {
    self.exec_command("SUNIONSTORE", $destination, |@keys)
}

###### ! Commands/Sets #######

###### Commands/SortedSets #######

method zadd(Str:D $key, *@args, *%named --> Int:D) {
    my @newargs = Array.new;
    @args = @args.reverse;
    for @args {
        if $_ ~~ Pair {
            @newargs.push(.value);
            @newargs.push(.key);
        }
        else {
            @newargs.push($_);
        }
    }
    for %named {
        @newargs.push(.value);
        @newargs.push(.key);
    }
    if @newargs.elems % 2 != 0 {
        die "ZADD requires an equal number of values and scores";
    }
    self.exec_command("ZADD", $key, |@newargs)
}

method zcard(Str:D $key --> Int:D) {
    self.exec_command("ZCARD", $key)
}

# TODO support (1, -inf, +inf syntax, http://redis.io/commands/zcount
method zcount(Str:D $key, Real:D $min, Real:D $max --> Int:D) {
    self.exec_command("ZCOUNT", $key, $min, $max)
}

method zincrby(Str:D $key, Real:D $increment, $member --> Real:D) {
    self.exec_command("ZINCRBY", $key, $increment, $member)
}

method zinterstore(
  Str:D $destination,
        *@keys,
       :WEIGHTS(@weights),
       :AGGREGATE(@aggregate)
--> Int:D) {
    my @args = Array.new;
    if @weights.elems > 0 {
        @args.push("WEIGHTS");
        for @weights {
            @args.push($_);
        }
    }
    if @aggregate.elems > 0 {
        @args.push("AGGREGATE");
        for @aggregate {
            @args.push($_);
        }
    }
    self.exec_command("ZINTERSTORE", $destination, @keys.elems, |@keys, |@args)
}

# TODO return array of paires if WITHSCORES is set
method zrange(
  Str:D $key, Int:D $start, Int:D $stop, :WITHSCORES($withscores)
) {
    my @args = $key, $start, $stop;
    @args.append('WITHSCORES') if $withscores.defined;
    self.exec_command("ZRANGE", |@args)
}

# TODO return array of paires if WITHSCORES is set
method zrangebyscore(
   Str:D  $key,
  Real:D  $min,
  Real:D  $max,
         :WITHSCORES($withscores),
   Int   :OFFSET($offset),
   Int   :COUNT($count)
--> List:D) {
    if ($offset.defined and !$count.defined) or (!$offset.defined and $count.defined) {
        die "`offset` and `count` must both be specified.";
    }
    my @args = $key, $min, $max;
    @args.append("WITHSCORES") if $withscores.defined;
    @args.append("LIMIT") if ($offset.defined and $count.defined);
    @args.append($offset) if $offset.defined;
    @args.append($count) if $count.defined;

    self.exec_command("ZRANGEBYSCORE", |@args)
}

method zrank(Str:D $key, $member) {
    self.exec_command("ZRANK", $key, $member)
}

method zrem(Str:D $key, *@members --> Int:D) {
    self.exec_command("ZREM", $key, |@members)
}

method zremrangbyrank(Str:D $key, Int:D $start, Int:D $stop --> Int:D) {
    self.exec_command("ZREMRANGEBYRANK", $key, $start, $stop)
}

method zremrangebyscore(Str:D $key, Real:D $min, Real:D $max --> Int:D) {
    self.exec_command("ZREMRANGEBYSCORE", $key, $min, $max)
}

# TODO return array of paires if WITHSCORES is set
method zrevrange(Str:D $key, $start, $stop, :WITHSCORES($withscores)) {
    self.exec_command("ZREVRANGE",
      $key, $start, $stop, $withscores.defined ?? "WITHSCORES" !! Nil
    )
}

# TODO return array of paires if WITHSCORES is set
method zrevrangebyscore(
   Str:D  $key,
  Real:D  $min,
  Real:D  $max,
         :WITHSCORES($withscores),
   Int   :OFFSET($offset),
   Int   :COUNT($count)
--> List:D) {
    if ($offset.defined and !$count.defined) or (!$offset.defined and $count.defined) {
        die "`offset` and `count` must both be specified.";
    }
    self.exec_command("ZREVRANGEBYSCORE", $key, $min, $max,
        $withscores.defined ?? "WITHSCORES" !! Nil,
        ($offset.defined and $count.defined) ?? "LIMIT" !! Nil,
        $offset.defined ?? $offset !! Nil,
        $count.defined ?? $count !! Nil
    )
}

method zrevrank(Str:D $key, $member) {
    self.exec_command("ZREVRANK", $key, $member)
}

method zscore(Str:D $key, $member --> Real:D) {
    self.exec_command("ZSCORE", $key, $member)
}

method zunionstore(
   Str:D  $destination,
          *@keys,
         :WEIGHTS(@weights),
         :AGGREGATE(@aggregate)
--> Int:D) {
    my @args = Array.new;
    if @weights.elems > 0 {
        @args.push("WEIGHTS");
        for @weights {
            @args.push($_);
        }
    }
    if @aggregate.elems > 0 {
        @args.push("AGGREGATE");
        for @aggregate {
            @args.push($_);
        }
    }
    self.exec_command("ZUNIONSTORE", $destination, @keys.elems, |@keys, |@args)
}

###### ! Commands/SortedSets #######

###### Commands/Pub&Sub #######

method psubscribe(*@patterns) {
    self.exec_command("PSUBSCRIBE", |@patterns)
}

method publish(Str:D $channel, $message) {
    self.exec_command("PUBLISH", $channel, $message)
}

method punsubscribe(*@patterns) {
    self.exec_command("PUNSUBSCRIBE", |@patterns)
}

method subscribe(*@channels) {
    self.exec_command("SUBSCRIBE", |@channels)
}

method unsubscribe(*@channels) {
    self.exec_command("UNSUBSCRIBE", |@channels)
}

###### ! Commands/Pub&Sub #######

###### Commands/Transactions #######

method discard(--> Bool:D) {
    self.exec_command("DISCARD")
}

# TODO format response according each command
method exec(--> List:D) {
    self.exec_command("EXEC")
}

method multi(--> Bool:D) {
    self.exec_command("MULTI")
}

method unwatch(--> Bool:D) {
    self.exec_command("UNWATCH")
}

method watch(*@keys --> Bool:D) {
    self.exec_command("WATCH")
}

###### ! Commands/Transactions #######

###### Commands/Scripting #######

method eval(Str:D $script, Int:D $numkeys, *@keys_and_args) {
    self.exec_command("EVAL", $script, $numkeys, |@keys_and_args)
}

method evalsha(Str:D $sha1, Int:D $numkeys, *@keys_and_args) {
    self.exec_command("EVALSHA", $sha1, $numkeys, |@keys_and_args)
}

method script_exists(*@scripts --> List:D) {
    self.exec_command("SCRIPT EXISTS", |@scripts)
}

method script_flush(--> Bool:D) {
    self.exec_command("SCRIPT FLUSH")
}

method script_kill(--> Bool:D) {
    self.exec_command("SCRIPT KILL")
}

method script_load(Str $script) {
    self.exec_command("SCRIPT LOAD", $script)
}

###### ! Commands/Scripting #######

# vim: expandtab shiftwidth=4
