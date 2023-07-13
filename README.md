# Any-Sync Node
Implementation of node from [`any-sync`](https://github.com/anyproto/any-sync) protocol.

## Building the source
To ensure compatibility, please use Go version `1.19`.

To build and run the Any-Sync Node on your own server, follow these steps:

1.  Clone `any-sync-node` repository.
2.  Navigate to the root directory of the repository.
3.  Run the following commands to install the required dependencies and build the Any-Sync Node.
    ```
    make deps
    make build
    ```
4.  If there are no errors, the Any-Sync Node will be built and can be found in the `/bin` directory.

## Running
Any-Sync Node requires a configuration. You can generate configuration files for your nodes with [`any-sync-network`](https://github.com/anyproto/any-sync-tools) tool.

The following options are available for running the Any-Sync Node:

 - `-c` — path to config file (default `etc/any-sync-node.yml`). 
 - `-v` — current version.
 - `-h` — help message.

## Graph example of using Any-Sync Nodes group

```mermaid
graph LR
A((Node1)) -- Sync --> B((Node2))
B -- Sync --> C((Node3))
C -- Sync --> A
U[Client] --> A

```

## Contribution
Thank you for your desire to develop Anytype together. 

Currently, we're not ready to accept PRs, but we will in the nearest future.

Follow us on [Github](https://github.com/anyproto) and join the [Contributors Community](https://github.com/orgs/anyproto/discussions).

---
Made by Any — a Swiss association 🇨🇭

Licensed under [MIT License](./LICENSE).
