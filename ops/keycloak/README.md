# Keycloak Bootstrap

This directory holds the local Keycloak bootstrap artifacts for the `eventide` realm.

- `realm-import/eventide-realm.json` defines the base realm, OIDC clients, and realm roles.
- No static users or credentials are committed here.
- Service accounts are modeled in the application database and should be mirrored into Keycloak clients by later platform automation.

Expected defaults:
- issuer: `http://localhost:8081/realms/eventide`
- audience: `eventide-api`
