# Integrator.Api image. Configuration is not baked in: mount an env file and
# pass it with --config_file; secrets come as environment variables, which win
# over the file. Database migrations (schema/postgresql/migration) are not run
# by this image.

FROM golang:1.24.13 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app .

# Static binary on distroless: CA certificates, no shell, runs as non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
EXPOSE 8080
ENTRYPOINT ["/app"]
CMD ["--config_file", "/config/app.env"]
