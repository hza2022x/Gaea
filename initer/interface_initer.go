package initer

type Initer interface {
	InitConfig()

	InitPhysicalDs()

	InitDataSource()
}
