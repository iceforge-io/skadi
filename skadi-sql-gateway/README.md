# skadi-sql-gateway

> NOTE: password mode is for local/dev only right now.

- `password`: cleartext password auth against `skadi.sql-gateway.pgwire.auth.users`.
- `trust` (default): accepts any username.

Configured under `skadi.sql-gateway.pgwire.auth.mode`:

### Auth modes

```
#   select 1;
# then:
psql -h 127.0.0.1 -p 15432 -U test postgres
# in another shell

.\mvnw.cmd -pl skadi-sql-gateway -am spring-boot:run
# run the app (gateway has both HTTP + pgwire listeners)

.\mvnw.cmd -pl skadi-sql-gateway -am test
# from repo root
```powershell

### Try with psql

- pgwire (PostgreSQL protocol): `15432`
- HTTP (Spring Boot): `8090`

### Default ports

This module contains a minimal PostgreSQL protocol listener intended to unblock Tableau/JDBC connectivity.

## pgwire (PostgreSQL wire protocol) MVP

Minimal SQL gateway service.

