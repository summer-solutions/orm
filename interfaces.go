package orm

type DefaultValuesInterface interface {
	SetDefaults()
}

type ValidateInterface interface {
	Validate() error
}

type AfterSavedInterface interface {
	AfterSaved(engine *Engine) error
}
