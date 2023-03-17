package server

type Node interface {
	Read() chan []byte
	Run()
	Write([]byte)
}
