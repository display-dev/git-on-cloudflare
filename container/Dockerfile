FROM golang:1.23.2-alpine@sha256:9dd2625a1ff2859b8d8b01d8f7822c0f528942fe56cfe7a1e7c38d3b8d72d679 AS build

WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/repository-git ./cmd/repository-git

FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

RUN apk add --no-cache git ca-certificates \
  && addgroup -S gitservice \
  && adduser -S -G gitservice -h /home/gitservice gitservice
COPY --from=build /out/repository-git /usr/local/bin/repository-git

USER gitservice
WORKDIR /home/gitservice
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/repository-git"]
