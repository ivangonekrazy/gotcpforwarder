/*
    TCP Forwarder in Golang
    Author: Ivan Tam <ivan@hipnik.net>
*/
package main

import (
    "net"
    "time"
    "fmt"
)

func main() {

    listenPort := ":1200"
    destination := ":1201"

    tcpAddr, _ := net.ResolveTCPAddr("ip4", listenPort)
    listener, _ := net.ListenTCP("tcp", tcpAddr)

    receiveStatusCh := make(chan int, 1)
    forwardStatusCh := make(chan int, 1)

    // emit a report periodically
    tickerCh := time.Tick(10 * time.Second)
    go statusStatus(receiveStatusCh, forwardStatusCh, tickerCh)

    // setup the forwarder; make a large queue for it 
    forwarderCh := make(chan []byte, 1000)
    go forwarder(destination, forwarderCh, forwardStatusCh)

    for { // enter listen loop
        conn, err := listener.AcceptTCP()
        if err != nil {
            continue
        }

        go handleReceive(conn, forwarderCh, receiveStatusCh)
    }
}

func statusStatus(rCh <-chan int, fCh <-chan int, tickCh <-chan time.Time) {
    var recvBytes int
    var recvMsgs int
    var fwdBytes int
    var fwdMsgs int

    ok := true
    for ok {
        select {
            case rBytes, _ := <-rCh:
                recvBytes += rBytes
                recvMsgs += 1
            case fBytes, _ := <-fCh:
                fwdBytes += fBytes
                fwdMsgs += 1
            case tick, _ := <-tickCh:
                fmt.Println(tick)
                fmt.Println("\tmessages recv:", recvMsgs, "\tbytes recv:", recvBytes)
                fmt.Println("\tmessages forwarded:", fwdMsgs, "\tbytes forwarded:", fwdBytes)
        }
    }
}

func forwarder(destination string, incomingCh <-chan []byte, reportCh chan<- int) {

    var conn net.Conn

    for { // retry until we can connect
        c, err := net.Dial("tcp", destination)
        if err == nil {
            conn = c
            fmt.Println("FORWARDER\tConnected to forward host", destination)
            break
        }
        fmt.Println("FORWARDER\tCan't connect to", destination, "yet...")
        time.Sleep( time.Second * 3 )
    }

    defer conn.Close()

    for {
        l, err := conn.Write( <-incomingCh )
        if err != nil {
            fmt.Println("FORWARDER\tError:", err)
            return
        }
        reportCh <- l
    }

}

func handleReceive(conn *net.TCPConn, forwarderCh chan<- []byte, reportCh chan<- int) {

    defer conn.Close() // always close a connection when we're done here

    var buf [512]byte

    for {
        n, readErr := conn.Read( buf[0:] )
        if readErr != nil {
            fmt.Println("RECEIVER\tConnection closed:", conn.RemoteAddr().String())
            return
        }

        echo := fmt.Sprintf("[%s] %s", time.Now(), buf[0:n])
        _, writeErr := conn.Write( []byte(echo) )
        if writeErr != nil {
            fmt.Println("RECEIVER\tError echoing:", writeErr)
            return
        }

        reportCh <- n
        forwarderCh <- buf[0:n]
    }
}
