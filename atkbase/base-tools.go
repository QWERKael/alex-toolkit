package atkbase

type ConnInfo struct {
	Host, Port, Database, User, Password, MutliAddr string
}

type DBEntity interface {
	Connect(ci ConnInfo) (conn interface{},err error)
}

