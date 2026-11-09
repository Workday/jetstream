# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it using [GitHub's private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability). Go to the **Security** tab of this repository and click **Report a vulnerability**.

Please do not report vulnerabilities through public issues, discussions, or pull requests.

Please include:

- The connector or component affected (for example `connectors/Zoom` or `control_plane_standards`)
- A description of the vulnerability
- Steps to reproduce
- Potential impact
- The version, branch, or commit you tested
- Suggested fix (if any)

Do not include real credentials, tokens, or customer data in a report. If you believe a credential has been exposed, tell us where it appears and we will remove it and rotate it.

> **Note:** Jetstream is open source software provided as is, and security reports are handled by project maintainers on a best-effort basis. We will acknowledge your report, investigate, and keep you informed of progress. Please allow reasonable time for a fix before disclosing an issue publicly.

## Scope

This policy covers the code and documentation in this repository. Vulnerabilities in Databricks, in the source systems the connectors read from, or in third-party libraries should be reported to those vendors or projects.

## Security Model

Jetstream is designed so that connectors hold **no long-lived secrets** in code, configuration, or the control plane:

- Connectors run as Databricks Jobs inside your own workspace, under the identity you configure for the job. Source data moves directly between the source system and your storage, with no third-party service in the path.
- Source system credentials live in a **Databricks secret scope**. Connectors receive only the name of the secret scope as a job parameter and read the credential values at runtime.
- The control plane never stores a credential value. Job parameters, connector state, and log event data may contain references such as a secret scope name, but never the secrets themselves.
- The control plane holds run history, state, and logs only. It never holds the business data a connector syncs.
- What a connector can read and write is bounded by the **grants you give the job's identity** in Unity Catalog and by the **permissions of the source system credentials** you provide.

## Best Practices for Deployments

- Never commit credentials, tokens, `.env` files, or workspace-specific identifiers and hostnames to version control, job parameters, notebooks, or test fixtures.
- Store all source system credentials in a Databricks secret scope, and restrict who can read that scope.
- Grant the job's identity the **minimum** Unity Catalog privileges it needs. Connectors only insert and update rows in the control plane tables, so they do not need privileges to create, alter, or drop them.
- Use read-only, narrowly scoped API tokens or service accounts for source systems wherever the source supports it, and rotate them on a regular schedule.
- Remember that **synced data may be sensitive**. Sources such as HR, contract, ticketing, and collaboration systems can contain personal or confidential information. Apply your Unity Catalog governance controls (grants, row filters, column masks) to the destination tables, and note that files landed in a Volume are governed by that Volume's permissions.
- Keep sensitive payloads out of logs and out of `connector_log.event_data`. If you write a new connector or modify an existing one, review what it logs.
- Keep dependencies current by running a tool such as `pip-audit` or `pip list --outdated` periodically for each connector you run.
