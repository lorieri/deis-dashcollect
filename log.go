package main

import (
	"fmt"
	"time"
	"strings"
	"bufio"
	"gopkg.in/redis.v2"
	"strconv"
	"os"
	"net"
	"github.com/coreos/go-etcd/etcd"
)

var etcdServers []string
var redisServer string

func main() {
        getetcdServers := getopt("ETCD_HOSTS", "")
        if getetcdServers == "" {
                panic("Please set ETCD_HOSTS environment, comma separated http:// hosts with port")
        }
        etcdServers = strings.Split(getetcdServers, ",")
        client := etcd.NewClient(etcdServers)
        fmt.Println(client)
        fmt.Println("Etcd Servers:")
        fmt.Println(etcdServers)
        setRedis()

	for {
		readlog()
		time.Sleep(time.Second * 1)
	}
}

// http://blog.gopheracademy.com/advent-2013/day-06-service-discovery-with-etcd/
func updateRedis(){
        client := etcd.NewClient(etcdServers)
        watchChan := make(chan *etcd.Response)
        go client.Watch("/deis-dashboard/redis", 0, false, watchChan, nil)
        resp := <-watchChan
        redisServer = strings.TrimSpace(resp.Node.Value)
        updateRedis()
}

func setRedis(){
        redisServer = getopt("REDIS_SERVER", "")
        fmt.Println("Set redisServer as "+redisServer)
        if redisServer == "" {
                client := etcd.NewClient(etcdServers)
                resp, err := client.Get("/deis-dashboard/redis", false, false)
                if err != nil {
                        panic(err)
                }
                fmt.Println("Set Redis Server as "+resp.Node.Value)
                redisServer = strings.TrimSpace(resp.Node.Value)
                go updateRedis()
        }
}

// copied from github.com/deis
func getopt(name, dfault string) string {
	value := os.Getenv(name)
	if value == "" {
		value = dfault
	}
	return value
}

func readlog() {

	logspout := getopt("LOGSPOUT","")

	// redis
        rc := redis.NewClient(&redis.Options{Network: "tcp", Addr: redisServer})

        conn, err := net.Dial("tcp", logspout)
        if err != nil {
                // handle error
		panic(err)
        }
        fmt.Fprintf(conn, "GET /logs/filter:deis-router?colors=off HTTP/1.0\r\n\r\n")
        scanner := bufio.NewScanner(conn)
        for scanner.Scan() {
		line := strings.Split(scanner.Text()," - ")

		if len(line) == 11 {
			// format  := `$deis_time $deis_unit: [$level] - [$time_local] - $remote_addr - $remote_user - $status - "$request" - $bytes_sent - "$http_referer" - "$http_user_agent" - "$server_name" - $upstream_addr`

			// deis			0       deis-router|[INFO]
			// time_local		1  - [15/Jan/2015:03:23:22 +0000]
			// remote_addr		2  - 10.134.66.67
			// remote_user		3  - -
			// status		4  - 200
			// request		5  - "GET / HTTP/1.1"
			// bytes_sent		6  - 872
			// http_referer		7  - "-"
			// http_user_agent	8  - "curl/7.30.0"
			// server_name		9  - ""
			// upstream_addr	10 - -


			upstream_addr := line[10]
			remote_addr := line[2]
			time_local := line[1]
			status := line[4]
			bytes_sent_str := line[6]
			bytes_sent, _ := strconv.ParseInt(bytes_sent_str, 0, 64)
			bytes_sent_float,_ := strconv.ParseFloat(bytes_sent_str,64)
			// fmt.Printf("%+v\n",bytes_sent)
			http_referer := line[7]
			request := line[5]
			// fmt.Printf("%q\n", strings.Split("a,b,c", ",")[0])
			server_name := line[9]
			if strings.Contains(server_name, "^") {
				server_name = strings.Split(server_name, "^")[1]
				server_name = strings.Split(server_name, `\`)[0]
			}else{
				server_name = "UNKNOWN"
			}
			// ZIncrBy(key string, increment int, member string)

			// global
			rc.ZIncrBy("current_z_top_upstream", 1, upstream_addr)
			rc.ZIncrBy("current_z_top_apps", 1, server_name)
			if !strings.HasPrefix(status, "2") && !strings.HasPrefix(status, "3"){
				rc.ZIncrBy("current_z_top_error_app_status",1,server_name+"_"+status)
				rc.IncrBy("current_k_total_errors", 1)
			}
			rc.ZIncrBy("current_z_top_remote_addr_status", 1, status+" "+remote_addr)
			rc.ZIncrBy("current_z_top_remote_addr_bytes_sent", bytes_sent_float, remote_addr)
			rc.ZIncrBy("current_z_top_apps_bytes_sent", bytes_sent_float, server_name)
			rc.IncrBy("current_k_total_bytes", bytes_sent)
			rc.IncrBy("current_k_total_requests", 1)
			rc.Set("current_s_last_log_time", time_local)



			// apps
			rc.ZIncrBy("current_z_top_app_upstream_"+server_name,1,upstream_addr)
			rc.ZIncrBy("current_z_top_app_upstream_status_"+server_name,1, status+" - "+upstream_addr)
			rc.ZIncrBy("current_z_top_app_request_"+server_name, 1, request)
			rc.ZIncrBy("current_z_top_app_status_"+server_name, 1, status+" "+request)
			if !strings.HasPrefix(status, "2") && !strings.HasPrefix(status, "3"){
				rc.ZIncrBy("current_z_top_app_error_referer_"+server_name, 1 , status+" "+http_referer)
				rc.ZIncrBy("current_z_top_app_error_request_"+server_name, 1 , status+" "+request)
				rc.ZIncrBy("current_z_top_app_error_remote_addr_"+server_name, 1, status+" "+remote_addr)
				rc.IncrBy("current_k_total_app_errors_"+server_name, 1)
			}
			rc.ZIncrBy("current_z_top_remote_addr_status_"+server_name, 1, status+" "+remote_addr)
			rc.ZIncrBy("current_z_top_remote_addr_bytes_sent_"+server_name, bytes_sent_float, remote_addr)
			rc.ZIncrBy("current_z_top_app_referer_"+server_name, 1, http_referer)
			rc.IncrBy("current_k_total_app_bytes_sent_"+server_name, bytes_sent)
			rc.IncrBy("current_k_total_app_requests_"+server_name, 1)
                }else{
                        if strings.Contains(scanner.Text(), " [error] ") {
                                continue
                        }
                        fmt.Println(len(line)," Unable to parse this line: ",scanner.Text())
                }
	}
}
