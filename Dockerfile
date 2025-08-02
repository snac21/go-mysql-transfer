# 第一阶段：使用本地golang镜像
FROM golang:1.22 AS compiler

ENV GO111MODULE=on \
    GOPROXY=https://goproxy.cn,direct

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o transfer .

RUN mkdir publish && cp transfer publish && \
    cp app.yml publish && cp -r web/statics publish

# 第二阶段：使用本地alpine镜像
FROM alpine:latest

# 安装必要的依赖
RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=compiler /app/publish .

EXPOSE 8060

ENTRYPOINT ["./transfer", "-config", "app.yml"]