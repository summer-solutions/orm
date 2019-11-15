package orm

import "fmt"

type pager struct {
	currentPage int
	pageSize    int
}

type Pager interface {
	GetPageSize() int
	GetCurrentPage() int
	String() string
	IncrementPage()
}

func (pager pager) GetPageSize() int {
	return pager.pageSize
}

func (pager pager) GetCurrentPage() int {
	return pager.currentPage
}

func (pager pager) IncrementPage() {
	pager.currentPage++
}

func (pager pager) String() string {
	return fmt.Sprintf("LIMIT %d,%d", (pager.currentPage-1)*pager.pageSize, pager.pageSize)
}

func NewPager(currentPage int, pageSize int) Pager {
	return pager{currentPage, pageSize}
}
