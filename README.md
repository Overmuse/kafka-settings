# kafka-settings
Rust utility library for easily mapping environment variables into strongly-typed kafka settings, and creating kafka consumers and producers with sane defaults.
Environment variables need to be prefixed with `KAFKA__`. The following settings are currently supported:

- `BOOTSTRAP_SERVERS`: The kafka bootstrap server address. Currently you can only pass a single server, despite the name.
- `SECURITY_PROTOCOL`: One of `PLAINTEXT` or `SASL_SSL`. If `SASL_SSL`, the following are also required:
  - `SASL_USERNAME`
  - `SASL_PASSWORD`
- `GROUP_ID` (optional): The group id for consumers.
- `INPUT_TOPICS` (optional): A CSV list of kafka topics for consumers to subscribe to.
  
