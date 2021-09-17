FROM golang:1.17-alpine

RUN apk add gcc musl-dev gdal-dev

COPY . /src
WORKDIR /src/cmd/l2serv
RUN go build

EXPOSE 8081
CMD ["./l2serv"]
