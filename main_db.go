package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type linkInfo struct {
	url *string
	key *string
}

type threadInfo struct {
	eventAction int
	num         int
}

const (
	EVENT_START = iota
	EVENT_ADD
	EVENT_SUB
)

const MAXTHREAD = 5
const MAXVIDEONUM = 10

var (
	ptnFindLink  = regexp.MustCompile(`(http://|https://|http:\\/\\/|https:\\/\\/)+([\w]*.tumblr.com)`)
	ptnFindFrame = regexp.MustCompile(`class=\'tumblr_video_container\'[\w \'\:\;\<\>\=]*\<iframe src='([\w\:\/\.]*)'`)
	ptnFindVideo = regexp.MustCompile(`\<source src=\"([\w:\/\.]*)\" type=\"video\/mp4\"\>`)
)

var db *sql.DB = nil
var db_error error = nil

var FrameRead map[string]string = make(map[string]string)

var event chan linkInfo
var eventNone chan int = make(chan int)
var threadMonitor chan threadInfo = make(chan threadInfo)
var threadMonitorNone chan int = make(chan int)
var threadMonitorStatus chan bool = make(chan bool)
var threadMonitorNum chan int = make(chan int)
var ThreadNum int = 0
var ThreadStatus bool = true

func Get(url string) (content string, statusCode int) {

	timeout := time.Duration(20 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}

	resp, err1 := client.Get(url)
	if err1 != nil {
		statusCode = -100
		return
	}
	defer resp.Body.Close()
	data, err2 := ioutil.ReadAll(resp.Body)
	if err2 != nil {
		statusCode = -200
		return
	}
	statusCode = resp.StatusCode
	content = string(data)
	return
}

func GetByProxy(url_addr, proxy_addr string) (content string, statusCode int) {
	request, _ := http.NewRequest("GET", url_addr, nil)
	proxy, err := url.Parse(proxy_addr)
	if err != nil {
		statusCode = -1
		return
	}
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxy),
		},
	}

	resp, err := client.Do(request)
	if err != nil {
		statusCode = -2
		return
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		statusCode = -3
		return
	}

	statusCode = resp.StatusCode
	content = string(body)

	return
}

func Md5(s string) (str string) {

	h := md5.New()

	h.Write([]byte(s))
	cipherStr := h.Sum(nil)

	str = hex.EncodeToString(cipherStr)
	return
}

func File_put_contents(path string, url string) bool {

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	stat, err := f.Stat()
	if err != nil {
		return false
	}

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Range", "bytes="+strconv.FormatInt(stat.Size(), 10)+"-")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	written, err := io.Copy(f, resp.Body)
	if err != nil {
		return false
	}
	if written > 0 {
		return true
	}
	return false
}

func Array_unique(list *[]string) []string {
	var x []string = []string{}
	for _, i := range *list {
		if len(x) == 0 {
			x = append(x, i)
		} else {
			for k, v := range x {
				if i == v {
					break
				}
				if k == len(x)-1 {
					x = append(x, i)
				}
			}
		}
	}
	return x
}

func Empty(param ...interface{}) (flag bool) {

	fmt.Println(len(param))
	return
}

func Get_base_url(url string) (result string, statusCode int) {

	urlList := make([]string, 0)

	result, statusCode = Get(url)

	if statusCode != 200 {

		return
	}

	linkList := ptnFindLink.FindAllStringSubmatch(result, -1)

	for _, item := range linkList {
		urlList = append(urlList, item[2])
	}

	urlList_quique := Array_unique(&urlList)

	for _, item := range urlList_quique {
		key := Md5(item)

		rows, err := db.Query("select _key from go_link_queue where _key = ?", key)

		if err != nil {
			fmt.Println(1)
			log.Println(err)
		}

		if !rows.Next() {
			stmt, err := db.Prepare("INSERT INTO go_link_queue(_key, item,add_time) VALUES(?,?,?)")
			//defer stmt.Close()

			if err != nil {
				fmt.Println(2)
				log.Println(err)
				return
			}
			stmt.Exec(key, item, time.Now().Format("2006-01-02 15:04:05"))
			stmt.Close()
		}
		rows.Close()
	}

	return
}

func HandleRun(eventAction int) {
	event = make(chan linkInfo)
	for {

		if eventAction == EVENT_START {
			fmt.Println("URL采集初始化开始")
			eventAction = -1
			eventNone <- 0
		}

		handMsg := <-event
		stmt, err := db.Prepare("UPDATE go_link_queue SET status = ? WHERE _key = ?")
		//defer stmt.Close()

		if err != nil {
			fmt.Println(3)
			log.Println(err)
			return
		}

		stmt.Exec(1, *handMsg.key)
		stmt.Close()
		threadMonitor <- threadInfo{EVENT_ADD, 1}
		go ThreadRun("http://" + *handMsg.url)
		threadMonitorNum <- ThreadNum
	}
}

func ThreadRun(url string) {

	defer func() {
		threadMonitor <- threadInfo{EVENT_SUB, 1}
		threadMonitorStatus <- true
	}()

	var FrameQueue map[string]string = make(map[string]string)

	result, statusCode := Get_base_url(url)
	if statusCode == 200 {
		IframeList := ptnFindFrame.FindAllStringSubmatch(result, -1)
		VideoList := ptnFindVideo.FindAllStringSubmatch(result, -1)

		AppendVideoList(VideoList)

		for _, value := range IframeList {
			key := Md5(value[1])
			_, queue_ok := FrameRead[key]
			if queue_ok {
				continue
			}
			FrameQueue[key] = value[1]
			FrameRead[key] = value[1]
		}

		for FrameKey, FrameValue := range FrameQueue {
			FrameResult, statusCode := Get(FrameValue)
			delete(FrameQueue, FrameKey)
			if statusCode != 200 {
				continue
			}
			VideoListT := ptnFindVideo.FindAllStringSubmatch(FrameResult, -1)
			AppendVideoList(VideoListT)
		}
	}
}

func AppendVideoList(list [][]string) {
	for _, item := range list {
		key := Md5(item[1])

		rows, err := db.Query("select _key from go_video_queue where _key = ?", key)

		if err != nil {
			fmt.Println(4)
			log.Println(err)
		}

		if !rows.Next() {
			stmt, err := db.Prepare("INSERT INTO go_video_queue(_key, item,add_time) VALUES(?,?,?)")
			//defer stmt.Close()

			if err != nil {
				fmt.Println(5)
				log.Println(err)
				return
			}
			stmt.Exec(key, item[1], time.Now().Format("2006-01-02 15:04:05"))
			stmt.Close()
		}
		rows.Close()
	}
}

func ThreadUrlDown() {
	for {

		ThreadNumT := 0

		rows, err := db.Query("select _key,item from go_link_queue where status = ? limit 10", 0)

		if err != nil {
			fmt.Println(6)
			log.Println(err)
		}

		for rows.Next() {

			var mapUrl *string
			var mapKey *string

			rows.Scan(&mapKey, &mapUrl)

			var flag bool = true
			for {

				if ThreadStatus {
					select {

					case <-threadMonitorStatus:
						flag = false
						if ThreadNum < MAXTHREAD {
							flag = true
						}
					default:
						if ThreadNumT < MAXTHREAD {
							flag = true
						} else {
							fmt.Printf("URL采集线程满,当前线程数:%d\n", ThreadNumT)
							flag = false
							time.Sleep(time.Second * 1)
						}
					}

					if flag {
						break
					}
				} else {
					time.Sleep(time.Second * 1)
				}
			}
			event <- linkInfo{mapUrl, mapKey}
			ThreadNumN := <-threadMonitorNum
			ThreadNumT = ThreadNumN

		}
		rows.Close()
		time.Sleep(time.Second * 1)
	}
}

func ThreadVideoDown() {
	for {
		ThreadStatus = true

		var videoNum *int

		rows1, err := db.Query("select count(id) from go_video_queue where status = ?", 0)

		if err != nil {
			log.Println(err)
		}

		rows1.Next()
		rows1.Scan(&videoNum)

		if *videoNum > MAXVIDEONUM {
			fmt.Println("URL采集线程暂停,开始采集视频")
			ThreadStatus = false

			var VideoValue *string
			var VideoKey *string

			rows, err := db.Query("select _key,item from go_video_queue where status = ?", 0)

			if err != nil {
				fmt.Println(7)
				log.Println(err)
			}

			for rows.Next() {
				rows.Scan(&VideoKey, &VideoValue)
				FilePath := "./video/" + *VideoKey
				DownStatus := File_put_contents(FilePath, *VideoValue)

				stmt, err := db.Prepare("UPDATE go_video_queue SET status = ? WHERE _key = ?")
				//defer stmt.Close()

				if err != nil {
					log.Println(err)
					return
				}

				stmt.Exec(1, *VideoKey)
				stmt.Close()

				if DownStatus {
					fmt.Printf("视频:%s下载成功\n", *VideoValue)
				} else {
					err := os.Remove(FilePath)
					if err != nil {
						fmt.Println("file remove Error!")
						fmt.Printf("%s\n", err)
					}
				}
			}
			rows.Close()
		}

		rows1.Close()
		time.Sleep(time.Second * 2)
	}
}

func ThreadMonitor(eventAction int) {

	for {

		if eventAction == EVENT_START {
			fmt.Println("线程监控初始化开始")
			eventAction = -1
			threadMonitorNone <- 0
		}
		ThreadQueue := <-threadMonitor
		if ThreadQueue.eventAction == EVENT_ADD {
			ThreadNum = ThreadNum + int(ThreadQueue.num)
		}
		if ThreadQueue.eventAction == EVENT_SUB {
			ThreadNum = ThreadNum - int(ThreadQueue.num)
		}
	}
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	db, db_error = sql.Open("mysql", "root:root@tcp(127.0.0.1:3306)/test1")

	if db_error != nil {
		log.Fatalf("连接数据库失败: %s\n", db_error)
	}

	defer db.Close()

	db_error = db.Ping()
	if db_error != nil {
		log.Fatal(db_error)
	}
	runtime.Breakpoint()
	fmt.Println("监听信号打开")
	c := make(chan os.Signal)
	signal.Notify(c)

	go ThreadMonitor(EVENT_START)
	<-threadMonitorNone
	fmt.Println("线程监控初始化完成")
	go HandleRun(EVENT_START)
	<-eventNone
	fmt.Println("URL采集初始化完成")

	go ThreadRun("https://www.tumblr.com/search/小学生")
	<-threadMonitorStatus
	go ThreadUrlDown()
	go ThreadVideoDown()

	for {
		s := <-c
		fmt.Printf("程序退出(%s)\n", s)
		os.Exit(1)
	}
}
