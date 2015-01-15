FROM google/golang

RUN go get gopkg.in/redis.v2

ADD . /

RUN go build log.go

CMD ["/log"]
