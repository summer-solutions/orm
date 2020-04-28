package orm

func getDirtyBind(entity Entity) (is bool, bind map[string]interface{}, err error) {
	orm := entity.getORM()
	id := orm.attributes.getID()
	t := orm.attributes.elem.Type()
	if orm.attributes.delete {
		return true, nil, nil
	}
	bind, err = createBind(id, orm.tableSchema, t, orm.attributes.elem, orm.dBData, "")
	if err != nil {
		return false, nil, err
	}
	is = id == 0 || len(bind) > 0
	return is, bind, nil
}
