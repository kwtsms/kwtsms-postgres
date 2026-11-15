# FILE:      Dockerfile
# CREATED:   2026-11-16 01:56 +03
# TASK:      int-postgres
# AUTHOR:    M Alhassoun
# WHAT:      Builds the kwtsms-pg-worker static binary and runs it on distroless/static as a non-root user.
# WHY:       A tiny image with no shell and no package manager; configuration by environment only.
# USAGE:     docker build -t kwtsms-pg-worker .
#            docker run --rm -e DATABASE_URL=... -e KWTSMS_USERNAME=... -e KWTSMS_PASSWORD=... kwtsms-pg-worker

FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/kwtsms-pg-worker ./cmd/kwtsms-pg-worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kwtsms-pg-worker /kwtsms-pg-worker
USER nonroot:nonroot
ENTRYPOINT ["/kwtsms-pg-worker"]
