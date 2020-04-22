package orm

type DefaultValuesInterface interface {
	SetDefaults()
}

type AfterSavedInterface interface {
	AfterSaved(engine *Engine) error
}
