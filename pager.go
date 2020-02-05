package orm

type Pager struct {
	CurrentPage int
	PageSize    int
}

func (pager *Pager) GetPageSize() int {
	return pager.PageSize
}

func (pager *Pager) GetCurrentPage() int {
	return pager.CurrentPage
}

func (pager *Pager) IncrementPage() {
	pager.CurrentPage++
}
