[![Actions Status](https://github.com/raku-community-modules/Redis/actions/workflows/linux.yml/badge.svg)](https://github.com/raku-community-modules/Redis/actions) [![Actions Status](https://github.com/raku-community-modules/Redis/actions/workflows/macos.yml/badge.svg)](https://github.com/raku-community-modules/Redis/actions)

NAME
====

Redis - a Raku binding for Redis

SYNOPSIS
========

```raku
use Redis;

my $redis = Redis.new("127.0.0.1:6379");
$redis.set("key", "value");
say $redis.get("key");
say $redis.info;
$redis.quit;
```

DESCRIPTION
===========

Redis provides a Raku interface to the [Redis](https://en.wikipedia.org/wiki/Redis) server.

METHODS
=======

new
---

```raku
method new(Str $server?, Str :$encoding?, Bool :$decode_response?)
```

Returns the redis object.

exec_command
------------

```raku
method exec_command(Str $command, *@args) returns Any
```

Executes arbitrary command.

AUTHORs
=======

  * Yecheng Fu

  * Raku Community

COPYRIGHT AND LICENSE
=====================

Copyright 2012 - 2018 Yecheng Fu

Copyright 2024 - 2026 Raku Community

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

