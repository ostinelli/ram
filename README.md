![CI](https://github.com/ostinelli/ram/actions/workflows/ci.yml/badge.svg) [![Hex pm](https://img.shields.io/hexpm/v/ram.svg)](https://hex.pm/packages/ram)

# Ram
**Ram** is an in-memory distributed KV store for Erlang and Elixir.

Ram uses the [Raft Consensus Algorithm](https://raft.github.io/) and is based on [Ra](https://github.com/rabbitmq/ra).

Ram is at an early development stage.

[[Documentation](https://hexdocs.pm/ram/)]

## Installation

### For Elixir
Add it to your deps:

```elixir
defp deps do
  [{:ram, "~> 0.1"}]
end
```

### For Erlang
If you're using [rebar3](https://github.com/erlang/rebar3), add `ram` as a dependency in your project's `rebar.config` file:

```erlang
{deps, [
  {ram, {git, "git://github.com/ostinelli/ram.git", {tag, "0.1.0"}}}
]}.
```
Or, if you're using [Hex.pm](https://hex.pm/) as package manager (with the [rebar3_hex](https://github.com/hexpm/rebar3_hex) plugin):

```erlang
{deps, [
  {ram, "0.1.0"}
]}.
```

Ensure that `ram` is started with your application, for example by adding it in your `.app` file to the list of `applications`:

```erlang
{application, my_app, [
    %% ...
    {applications, [
        kernel,
        stdlib,
        sasl,
        ram,
        %% ...
    ]},
    %% ...
]}.
```

## Contributing
So you want to contribute? That's great! Please follow the guidelines below. It will make it easier to get merged in.

Before implementing a new feature, please submit a ticket to discuss what you intend to do. Your feature might already be in the works, or an alternative implementation might have already been discussed.

Do not commit to master in your fork. Provide a clean branch without merge commits. Every pull request should have its own topic branch. In this way, every additional adjustments to the original pull request might be done easily, and squashed with `git rebase -i`. The updated branch will be visible in the same pull request, so there will be no need to open new pull requests when there are changes to be applied.

Ensure that proper testing is included. To run Ram tests you simply have to be in the project's root directory and run:

```bash
$ make test
```

## License

Copyright (c) 2021 Roberto Ostinelli.

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
