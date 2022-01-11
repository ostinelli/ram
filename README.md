![CI](https://github.com/ostinelli/ram/actions/workflows/ci.yml/badge.svg) [![Hex pm](https://img.shields.io/hexpm/v/ram.svg)](https://hex.pm/packages/ram)

# Ram
**Ram** is a distributed KV store for Erlang and Elixir.
It chooses Consistency over Availability by using the [Raft Consensus Algorithm](https://raft.github.io/),
and is based on Rabbit MQ's implementation [Ra](https://github.com/rabbitmq/ra).

[[Documentation](https://hexdocs.pm/ram/)]

## Installation

### For Elixir
Add it to your deps:

```elixir
defp deps do
  [{:ram, "~> 0.5"}]
end
```

### For Erlang
If you're using [rebar3](https://github.com/erlang/rebar3), add `ram` as a dependency in your project's `rebar.config` file:

```erlang
{deps, [
  {ram, {git, "git://github.com/ostinelli/ram.git", {tag, "0.5.0"}}}
]}.
```
Or, if you're using [Hex.pm](https://hex.pm/) as package manager (with the [rebar3_hex](https://github.com/hexpm/rebar3_hex) plugin):

```erlang
{deps, [
  {ram, "0.5.0"}
]}.
```

## Quick start

> Note: this example below assumes that you are familiar with [Distributed Erlang](https://www.erlang.org/doc/reference_manual/distributed.html).

### Elixir
Open 3 shells and start three Erlang nodes:

```bash
$ iex --name ram1@127.0.0.1 -S mix
```

```bash
$ iex --name ram2@127.0.0.1 -S mix
```

```bash
$ iex --name ram3@127.0.0.1 -S mix
```

Choose the main node (for instance `ram1@127.0.0.1`), and on that node run the following.

First, create an Erlang cluster by connecting all nodes:
```erlang
iex(ram1@127.0.0.1)1> nodes = [:"ram1@127.0.0.1", :"ram2@127.0.0.1", :"ram3@127.0.0.1"]
[:"ram1@127.0.0.1", :"ram2@127.0.0.1", :"ram3@127.0.0.1"]
iex(ram1@127.0.0.1)2> for node <- nodes, do: Node.connect(node)
[true, true, true]
```

Now we can create a Ram cluster running on these nodes:

```erlang
iex(ram1@127.0.0.1)3> :ram.start_cluster(nodes)
13:03:12.902 [info]  RAM[ram1@127.0.0.1] Cluster started on [:"ram1@127.0.0.1", :"ram2@127.0.0.1", :"ram3@127.0.0.1"]
:ok
```

You can now store and retrieve values from all the nodes:

```erlang
iex(ram1@127.0.0.1)4> :ram.put("key", "value")
:ok
iex(ram1@127.0.0.1)5> :ram.get("key")
"value"
```

```erlang
iex(ram2@127.0.0.1)5> :ram.get("key")
"value"
```

```erlang
iex(ram3@127.0.0.1)5> :ram.get("key")
"value"
```

### Erlang
Open 3 shells and start three Erlang nodes:

```bash
$ rebar3 shell --name ram1@127.0.0.1
```

```bash
$ rebar3 shell --name ram2@127.0.0.1
```

```bash
$ rebar3 shell --name ram3@127.0.0.1
```

Choose the main node (for instance `ram1@127.0.0.1`), and on that node run the following.

First, create an Erlang cluster by connecting all nodes:
```erlang
(ram1@127.0.0.1)1> Nodes = ['ram1@127.0.0.1', 'ram2@127.0.0.1', 'ram3@127.0.0.1'].
['ram1@127.0.0.1','ram2@127.0.0.1','ram3@127.0.0.1']
(ram1@127.0.0.1)2> [net_kernel:connect_node(Node) || Node <- Nodes].
[true,true,true]
```

Now we can create a Ram cluster running on these nodes:

```erlang
(ram1@127.0.0.1)3> ram:start_cluster(Nodes).
=INFO REPORT==== 4-Jan-2022::12:46:34.071524 ===
RAM[ram1@127.0.0.1] Cluster started on ['ram1@127.0.0.1','ram2@127.0.0.1',
'ram3@127.0.0.1']
ok
```

You can now store and retrieve values from all the nodes:

```erlang
(ram1@127.0.0.1)4> ram:put("key", "value"). 
ok
(ram1@127.0.0.1)5> ram:get("key").
"value"
```

```erlang
(ram2@127.0.0.1)1> ram:get("key").
"value"
```

```erlang
(ram3@127.0.0.1)1> ram:get("key").
"value"
```

## Configuration Options

### release_cursor_count
Specifies after how many logs `ram` should create a `ra` snapshot. Defaults to `1000`.

#### Elixir
  
```elixir
config :ram,
  release_cursor_count: 1000
```

#### Erlang

```erlang
{ram, [
  {release_cursor_count, 1000}
]}
```

### Other settings
Since Ram uses [Ra](https://github.com/rabbitmq/ra), please refer to Ra's
[documentation](https://github.com/rabbitmq/ra#configuration-reference) on available options.

## Contributing
So you want to contribute? That's great! Please follow the guidelines below. It will make it easier to get merged in.

Before implementing a new feature, please submit a ticket to discuss what you intend to do.
Your feature might already be in the works, or an alternative implementation might have already been discussed.

Do not commit to master in your fork. Provide a clean branch without merge commits.
Every pull request should have its own topic branch. In this way, every additional adjustments to the original pull request
might be done easily, and squashed with `git rebase -i`. The updated branch will be visible in the same pull request,
so there will be no need to open new pull requests when there are changes to be applied.

Ensure that proper testing is included. To run Ram tests you simply have to be in the project's root directory and run:

```bash
$ make test
```

## License

Copyright (c) 2021-2022 Roberto Ostinelli.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
