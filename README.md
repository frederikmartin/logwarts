# Logwarts

Welcome to **Logwarts**, an open-source command-line tool designed for efficient, magical and AWS Athena-like processing of AWS Application Load Balancer (ALB) log files. Inspired by the wizarding world, Logwarts aims to bring a bit of magic to your log analysis tasks.

**Hint:** If you have several GB of log data collected over a long period of time, Logwarts can process these logs efficiently without loading all data into memory, thanks to its streaming processing. However, for very large datasets, consider using AWS Athena to query the data.

## Features

- **Easy Parsing**: Quickly parse through various ALB log files with ease
- **Session Management**: Create and switch between multiple sessions, each with its own log table
- **Streaming Processing**: Efficiently processes logs without loading everything into memory
- **Advanced Filtering**: Use powerful filtering options to zero in on significant data
- **Customizable Output**: Display results in detailed or simple formats
- **Spellbinding Speed**: Blazingly™ fast processing of large log files
- **AWS ELB S3 Import**: Import files directly from S3 to avoid overhead

### Ideas

- CSV export
- Analytics report export
- Make available via Homebrew

## Prerequisites

To get the most out of Logwarts, ensure that:

- Access logging is enabled on your ALBs
- Log files are available locally or in an S3 bucket you can access

## Getting Started

### Installation

To install Logwarts, you can build it from source using Go. Follow the steps below:

1. **Install go**
    
    You can use `asdf` to install the required language dependencies defined in `.tool-versions` file:
    ```bash
    asdf install
    ```
2. **Clone the repository**

    ```bash
    git clone https://github.com/frederikmartin/logwarts.git
    cd logwarts
    ```
3. **Build the project**

    ```bash
    make build
    ```
    This will create an executable named `logwarts` in the project directory.
4. **Install the executable**
    Symlink the executable to a directory in your `$PATH` (e.g., `/usr/local/bin`):

    ```bash
    make symlink
    ```
    Restart your terminal session and you can start running `logwarts` from anywhere in your terminal.

### Usage

#### Session Management

Logwarts introduces a tmux-like session management model, allowing you to create, switch, and attach to multiple sessions, each having its own ALB log table. This feature provides greater flexibility when dealing with different sets of logs and contexts.

**Create New Session**
```bash
logwarts session create my_session
```

This command creates a new session named `my_session` and automatically sets it as active. All subsequent imports and queries will be tied to this session's ALB log table.

#### Session-based Log Import

When importing logs, Logwarts now dynamically creates a new ALB log table for each session, allowing you to maintain separate log data for different contexts. This eliminates the need to mix data from different sources or analysis sessions.

```bash
logwarts import --source=local --files=my_log_file.log
```

#### Querying Data from Active Session

All data imported during the active session will be accessible for queries. For example:
```bash
logwarts query "SELECT * FROM alb_logs LIMIT 10;"
```

This command retrieves the first 10 rows from the ALB log table associated with the active session `my_session`.

### Examples

See [AWS docs](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html) for available columns to filter by.

```bash
logwarts query "SELECT type, time, elb, request, target_processing_time, user_agent FROM alb_logs WHERE target_status_code = 200 LIMIT 5"
```

## Contributing

Contributions are what make the open-source community such a fantastic place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

Distributed under the MIT License. See [LICENSE](./LICENSE) for more information.

