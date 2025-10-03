This folder holds reusable Jinja macros to keep SQL DRY and readable.

- `surrogate_key.sql`: wraps dbt_utils.surrogate_key
- `clean_utils.sql`: common data cleansing helpers (timestamps/strings/status)
- `money_utils.sql`: money rounding and safe divide
- `time_utils.sql`: date/time helpers (date casts, hours diff)
- `pii_utils.sql`: email hashing (SHA256)
- `notify_slack.sql`: (optional) Slack notification on run failures
