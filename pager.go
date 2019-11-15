package orm

import "fmt"

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

func (pager Pager) String() string {
	return fmt.Sprintf("LIMIT %d,%d", (pager.CurrentPage-1)*pager.PageSize, pager.PageSize)
}
