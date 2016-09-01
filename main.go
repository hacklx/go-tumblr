package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"time"
)

type linkInfo struct {
	url string
	key string
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

var FrameRead map[string]string = make(map[string]string)
var linkQueue map[string]string = make(map[string]string)
var linkRead map[string]string = make(map[string]string)
var VideoQueue map[string]string = make(map[string]string)
var VideoRead map[string]string = make(map[string]string)
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
		_, read_ok := linkRead[key]
		if read_ok {
			continue
		}
		_, queue_ok := linkQueue[key]
		if queue_ok {
			continue
		}
		linkQueue[key] = item
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
		linkRead[handMsg.key] = handMsg.url
		fmt.Printf("当前URL队列:%d,当前入库URL:%d\n", len(linkQueue), len(linkRead))
		threadMonitor <- threadInfo{EVENT_ADD, 1}
		go ThreadRun("http://" + handMsg.url)
		delete(linkQueue, handMsg.key)
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
		fmt.Println("线程退出")
	}
}

func AppendVideoList(list [][]string) {
	for _, item := range list {
		key := Md5(item[1])
		_, read_ok := VideoRead[key]
		if read_ok {
			continue
		}
		_, queue_ok := VideoQueue[key]
		if queue_ok {
			continue
		}
		fmt.Printf("视频:%s进栈\n", item[1])
		VideoQueue[key] = item[1]
	}
}

func ThreadUrlDown() {
	for {
		func(queue map[string]string) {

			ThreadNumT := 0

			for mapKey, mapUrl := range queue {

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

			time.Sleep(time.Second * 1)

		}(linkQueue)
	}
}

func ThreadVideoDown() {
	for {
		ThreadStatus = true
		if len(VideoQueue) > MAXVIDEONUM {
			fmt.Println("URL采集线程暂停,开始采集视频")
			ThreadStatus = false
			for VideoKey, VideoValue := range VideoQueue {

				FilePath := "./video/" + VideoKey + ".mp4"
				DownStatus := File_put_contents(FilePath, VideoValue)

				VideoRead[VideoKey] = VideoValue
				delete(VideoQueue, VideoKey)

				if DownStatus {
					fmt.Printf("视频:%s下载成功\n", VideoValue)
				} else {
					err := os.Remove(FilePath)
					if err != nil {
						fmt.Println("file remove Error!")
						fmt.Printf("%s\n", err)
					}
				}
			}
		}
		time.Sleep(time.Second * 1)
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
	fmt.Println("监听信号打开")
	c := make(chan os.Signal)
	signal.Notify(c)

	go ThreadMonitor(EVENT_START)
	<-threadMonitorNone
	fmt.Println("线程监控初始化完成")
	go HandleRun(EVENT_START)
	<-eventNone
	fmt.Println("URL采集初始化完成")

	go ThreadRun("https://www.tumblr.com/search/萝莉")
	<-threadMonitorStatus
	go ThreadUrlDown()
	go ThreadVideoDown()

	for {
		s := <-c
		fmt.Printf("程序退出(%s)\n", s)
		os.Exit(1)
	}
}
