# Cloud Run Deployment Notes

## Secure invocation

- Set Cloud Run service to require authentication (IAM) OR keep it internal-only.
- This code also requires `x-agent-key` header matching `WATCHER_AGENT_KEY`.

## Scheduler trigger (example)

Use Cloud Scheduler to call `POST /poll` every minute.

- URL: https://<service-url>/poll
- Method: POST
- Headers:
  - x-agent-key: <WATCHER_AGENT_KEY>
  - content-type: application/json
- Body:
  - {"tenant":"hoamx_com","batch_size":50,"emit_full_row":false}

If you use IAM auth, configure Scheduler with OIDC token using a service account that has run.invoker.

## DB networking

Recommended:
- Cloud SQL private IP
- Serverless VPC Access connector
- DATABASE_URL points to private host/IP
