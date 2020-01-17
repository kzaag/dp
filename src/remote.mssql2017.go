package main

import (
	"container/list"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

type RemoteMsSql2017 struct{}

func (m RemoteMsSql2017) GetAllTableName(db *sql.DB) ([]string, error) {
	r, err := db.Query(
		"select s.name + '.' + t.name from sys.tables t inner join sys.schemas s on s.schema_id = t.schema_id")
	if err != nil {
		return nil, err
	}

	buff := list.New()
	for r.Next() {
		var t string
		err := r.Scan(&t)
		if err != nil {
			return nil, err
		}
		buff.PushBack(t)
	}

	var ret = make([]string, buff.Len())
	for i, x := 0, buff.Front(); i < buff.Len(); i, x = i+1, x.Next() {
		ret[i] = x.Value.(string)
	}

	return ret, nil
}

// split {schema}.{name} sql name into 2 parts
func SchemaName(name string) (string, string, error) {
	if name == "" || !strings.Contains(name, ".") {
		return "", "", fmt.Errorf("invalid sql name. expected: {schema}.{name} got: \"" + name + "\"")
	}
	sn := strings.Split(name, ".")
	if sn == nil || len(sn) != 2 {
		return "", "", fmt.Errorf("couldnt split sql name into schema, name array")
	}
	return sn[0], sn[1], nil
}

func (m RemoteMsSql2017) GetAllIx(db *sql.DB, tableName string) ([]Index, error) {

	schema, name, err := SchemaName(tableName)
	if err != nil {
		return nil, err
	}

	r, err := db.Query(
		`select 
		i.name,
		i.is_unique,
		i.type_desc
	 from
		sys.indexes i
		inner join sys.tables t on t.object_id = i.object_id
		inner join sys.schemas s on s.schema_id = t.schema_id
	where s.name = @s and 
		  t.name = @n and 
		  i.type <> 0 and
		  is_primary_key = 0 and 
		  is_unique_constraint = 0`, sql.Named("s", schema), sql.Named("n", name))
	if err != nil {
		return nil, err
	}
	tmp := list.New()

	for r.Next() {
		var k Index
		err := r.Scan(&k.Name, &k.Is_unique, &k.Type)
		if err != nil {
			panic(err)
		}
		tmp.PushBack(k)
	}

	ret := make([]Index, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		c := x.Value.(Index)
		r, err := db.Query(
			`select
				c.name, 
				ic.is_descending_key, 
				ic.is_included_column
			from sys.indexes i
				inner join sys.index_columns ic on ic.index_id = i.index_id and i.object_id = ic.object_id
				inner join sys.columns c on c.column_id = ic.column_id and ic.object_id = c.object_id
			where  i.name = @cname`, sql.Named("cname", c.Name))
		if err != nil {
			return nil, err
		}
		c.Columns, err = MapIxColumns(r)
		if err != nil {
			return nil, err
		}
		ret[i] = c
	}

	return ret, nil
}

func (m RemoteMsSql2017) GetAllUnique(db *sql.DB, tableName string) ([]Unique, error) {
	r, err := db.Query(
		`select kc.name
		from sys.key_constraints kc
		inner join sys.tables tt on tt.object_id = kc.parent_object_id
		inner join sys.schemas ss on ss.schema_id = tt.schema_id
		where ss.name + '.' + tt.name = @tname and kc.type = 'UQ'`, sql.Named("tname", tableName))
	if err != nil {
		return nil, err
	}
	tmp := list.New()

	for r.Next() {
		var k Unique
		err := r.Scan(&k.Name)
		if err != nil {
			return nil, err
		}
		tmp.PushBack(k)
	}

	ret := make([]Unique, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		c := x.Value.(Unique)
		r, err := db.Query(
			`select
				c.name, ic.is_descending_key
			from sys.indexes i
				inner join sys.index_columns ic on ic.index_id = i.index_id and i.object_id = ic.object_id
				inner join sys.columns c on c.column_id = ic.column_id and ic.object_id = c.object_id
			where i.is_unique_constraint = 1 and i.name = @cname
			`, sql.Named("cname", c.Name))
		if err != nil {
			return nil, err
		}
		c.Columns, err = MapConstraintCols(r)
		if err != nil {
			return nil, err
		}
		ret[i] = c
	}

	return ret, nil
}

func (m RemoteMsSql2017) GetAllCheck(db *sql.DB, tableName string) ([]Check, error) {
	r, err := db.Query(
		`select
			cc.name,
			cc.definition
		from sys.check_constraints cc
			inner join sys.tables t on t.object_id = cc.parent_object_id
			inner join sys.schemas s on s.schema_id = t.schema_id
		where s.name + '.' + t.name = @tname`, sql.Named("tname", tableName))
	if err != nil {
		return nil, err
	}
	tmp := list.New()

	for r.Next() {
		var k Check
		err := r.Scan(&k.Name, &k.Def)
		if err != nil {
			return nil, err
		}
		tmp.PushBack(k)
	}

	cs := make([]Check, tmp.Len())
	for i, x := 0, tmp.Front(); i < tmp.Len(); i, x = i+1, x.Next() {
		cs[i] = x.Value.(Check)
	}

	return cs, nil
}

func (m RemoteMsSql2017) GetAllColumn(db *sql.DB, tableName string) ([]Column, error) {
	r, err := db.Query(
		`select
			c.name,
			t.name as type,
			c.max_length,
			c.precision,
			c.scale,
			c.is_nullable,
			c.is_identity
		from sys.columns c
		inner join sys.tables tt on tt.object_id = c.object_id
		inner join sys.schemas s on s.schema_id = tt.schema_id
		inner join sys.types t on t.system_type_id = c.system_type_id and t.name != 'sysname'
		where s.name + '.' + tt.name =@table_name
		order by c.column_id`, sql.Named("table_name", tableName))
	if err != nil {
		return nil, err
	}

	buff := list.New()

	for r.Next() {
		var el Column
		err := r.Scan(
			&el.Name,
			&el.Type,
			&el.Max_length,
			&el.Precision,
			&el.Scale,
			&el.Is_nullable,
			&el.Is_Identity)
		if err != nil {
			return nil, err
		}
		buff.PushBack(el)
	}

	var ret = make([]Column, buff.Len())
	for i, x := 0, buff.Front(); i < buff.Len(); i, x = i+1, x.Next() {
		ret[i] = x.Value.(Column)
	}

	return ret, nil
}

func (m RemoteMsSql2017) GetAllFK(db *sql.DB, tableName string) ([]ForeignKey, error) {
	r, err := db.Query(
		`select
			s.name + '.' + t.name as ref_table,
			fk.name as name
		from sys.foreign_keys fk
			inner join sys.tables t on t.object_id = fk.referenced_object_id
			inner join sys.schemas s on s.schema_id = t.schema_id
			inner join sys.tables tt on tt.object_id = fk.parent_object_id
			inner join sys.schemas ss on ss.schema_id = tt.schema_id
		where ss.name + '.' + tt.name = @tname`, sql.Named("tname", tableName))
	if err != nil {
		return nil, err
	}
	tmp := list.New()

	for r.Next() {
		var k ForeignKey
		err := r.Scan(&k.Ref_table, &k.Name)
		if err != nil {
			return nil, err
		}
		tmp.PushBack(k)
	}

	ret := make([]ForeignKey, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		fk := x.Value.(ForeignKey)
		r, err := db.Query(
			`select fkcc.name , 0 as d
			from sys.foreign_keys fk
			inner join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
			inner join sys.columns fkcc on fkcc.column_id = fkc.parent_column_id and fkcc.object_id = fk.parent_object_id
			where fk.name = @fkname
			`, sql.Named("fkname", fk.Name))
		if err != nil {
			return nil, err
		}
		fk.Columns, err = MapConstraintCols(r)
		if err != nil {
			return nil, err
		}
		r, err = db.Query(
			`select fkcc.name, 0 as d
			from sys.foreign_keys fk
			inner join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
			inner join sys.columns fkcc on fkcc.column_id = fkc.referenced_column_id and fkcc.object_id = fk.referenced_object_id
			where fk.name = @fkname
			`, sql.Named("fkname", fk.Name))
		if err != nil {
			return nil, err
		}
		fk.Ref_columns, err = MapConstraintCols(r)
		if err != nil {
			return nil, err
		}
		ret[i] = fk
	}

	return ret, nil
}

func (m RemoteMsSql2017) GetAllPK(db *sql.DB, tableName string) (*PrimaryKey, error) {
	r, err := db.Query(`
	SELECT
	c.name
	FROM sys.tables t
	INNER JOIN sys.schemas s on s.schema_id = t.schema_id
	INNER JOIN sys.key_constraints c ON t.object_id = c.parent_object_id
	WHERE s.name + '.' + t.name =@tname and c.type = 'PK'`, sql.Named("tname", tableName))
	if err != nil {
		return nil, err
	}

	if !r.Next() {
		return nil, nil
	}

	var ret PrimaryKey
	r.Scan(&ret.Name)

	r, err = db.Query(
		`select
			c.name, ic.is_descending_key
	from sys.indexes i
		inner join sys.index_columns ic on ic.index_id = i.index_id and i.object_id = ic.object_id
		inner join sys.columns c on c.column_id = ic.column_id and ic.object_id = c.object_id
	where i.is_primary_key = 1 and i.name = @pkname`, sql.Named("pkname", ret.Name))

	tmp := list.New()

	for r.Next() {
		var column ConstraintColumn
		if err := r.Scan(&column.Name, &column.Is_descending); err != nil {
			return nil, err
		}
		tmp.PushBack(column)
	}

	cols := make([]ConstraintColumn, tmp.Len())
	for i, x := 0, tmp.Front(); i < tmp.Len(); i, x = i+1, x.Next() {
		cols[i] = x.Value.(ConstraintColumn)
	}

	ret.Columns = cols

	return &ret, nil
}

func CsColumnToString(m RemoteMsSql2017, c ConstraintColumn) string {
	ret := c.Name
	if c.Is_descending {
		ret += " DESC"
	}
	return ret
}

func ColumnTypeToString(column *Column) (string, error) {
	cs := ""
	switch strings.ToLower(column.Type) {
	case "nvarchar":
		if column.Max_length == -1 {
			cs += strings.ToUpper(column.Type) + "(MAX)"
		} else {
			cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(column.Max_length/2) + ")"
		}
	case "varbinary":
		fallthrough
	case "varchar":
		if column.Max_length == -1 {
			cs += strings.ToUpper(column.Type) + "(MAX)"
		} else {
			cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(column.Max_length) + ")"
		}
	case "int":
		fallthrough
	case "smallint":
		fallthrough
	case "tinyint":
		fallthrough
	case "bigint":
		fallthrough
	case "bit":
		fallthrough
	case "uniqueidentifier":
		cs += strings.ToUpper(column.Type)
	case "binary":
		cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(int(column.Max_length)) + ")"
	case "datetime2":
		cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(int(column.Scale)) + ")"
	case "decimal":
		cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(int(column.Precision)) + "," + strconv.Itoa(int(column.Scale)) + ")"
	default:
		panic("no mssql mapper for type : " + strings.ToLower(column.Type))
	}
	return cs, nil
}

func (m RemoteMsSql2017) ColumnToString(column *Column) (string, error) {
	var cs string

	cs += column.Name + " "

	c, e := ColumnTypeToString(column)
	if e != nil {
		return "", e
	}
	cs += c

	if !column.Is_nullable {
		cs += " NOT NULL"
	}

	if column.Is_Identity {
		cs += " IDENTITY"
	}

	return cs, nil
}

func (m RemoteMsSql2017) FkToString(fk *ForeignKey) (string, error) {
	if fk.Name == "" {
		return "", fmt.Errorf("foreign key name was empty")
	}

	if fk.Ref_table == "" {
		return "", fmt.Errorf("foreign key table was empty")
	}

	if fk.Ref_columns == nil || len(fk.Ref_columns) == 0 {
		return "", fmt.Errorf("foreign key reference column was empty")
	}

	if fk.Columns == nil || len(fk.Columns) < 1 {
		return "", fmt.Errorf("foreign key columns were not provided")
	}

	ret := "CONSTRAINT " + fk.Name + " FOREIGN KEY ("

	for i := 0; i < len(fk.Columns); i++ {
		ret += CsColumnToString(m, fk.Columns[i]) + ","
	}

	ret = strings.TrimSuffix(ret, ",")

	ret += ") REFERENCES " + fk.Ref_table + " ("

	for i := 0; i < len(fk.Ref_columns); i++ {
		ret += CsColumnToString(m, fk.Ref_columns[i]) + ","
	}

	ret = strings.TrimSuffix(ret, ",")
	ret += ")"

	return ret, nil
}

func (m RemoteMsSql2017) PkToString(pk *PrimaryKey) (string, error) {
	if pk.Name == "" {
		return "", fmt.Errorf("pk name was empty")
	}

	if pk.Columns == nil || len(pk.Columns) < 1 {
		return "", fmt.Errorf("invalid columns for primary key")
	}

	ret := "CONSTRAINT " + pk.Name + " PRIMARY KEY ("

	for i := 0; i < len(pk.Columns); i++ {
		ret += CsColumnToString(m, pk.Columns[i]) + ","
	}
	ret = strings.TrimSuffix(ret, ",")

	ret += ")"

	return ret, nil
}

func (m RemoteMsSql2017) UniqueToString(c *Unique) (string, error) {
	if c.Name == "" {
		return "", fmt.Errorf("pk name was empty")
	}

	if c.Columns == nil || len(c.Columns) < 1 {
		return "", fmt.Errorf("invalid columns for constraint")
	}

	ret := "CONSTRAINT " + c.Name + " UNIQUE ("

	for i := 0; i < len(c.Columns); i++ {
		ret += CsColumnToString(m, c.Columns[i]) + ","
	}
	ret = strings.TrimSuffix(ret, ",")

	ret += ")"

	return ret, nil
}

func (m RemoteMsSql2017) CheckToString(c *Check) (string, error) {
	if c.Name == "" {
		return "", fmt.Errorf("check constraint name was empty")
	}

	ret := "CONSTRAINT " + c.Name + " CHECK ("

	ret += c.Def

	ret += ")"

	return ret, nil
}

func (m RemoteMsSql2017) AddIxToString(tableName string, ix *Index) (string, error) {
	unique := ""
	if ix.Is_unique {
		unique = "UNIQUE "
	}

	t := "NONCLUSTERED "
	if ix.Type != "" {
		t = strings.ToUpper(ix.Type) + " "
	}

	ret :=
		"CREATE " + unique +
			t + "INDEX " +
			ix.Name + " ON " + tableName + " ("

	if ix.Columns == nil {
		return "", fmt.Errorf("on index: " + ix.Name + " no columns specified")
	}

	for i := 0; i < len(ix.Columns); i++ {
		c := ix.Columns[i]
		if c.Is_Included_column {
			continue
		}
		ret += c.Name
		if c.Is_descending {
			ret += " DESC"
		}
	}

	ret += ")"

	var includes bool = false
	for i := 0; i < len(ix.Columns); i++ {
		c := ix.Columns[i]
		if !c.Is_Included_column {
			continue
		}

		if !includes {
			ret += " INCLUDE ("
		}
		includes = true

		ret += c.Name
		if c.Is_descending {
			ret += " DESC"
		}
	}

	if includes {
		ret += ")"
	}

	ret += ";\n"

	return ret, nil
}

func (m RemoteMsSql2017) DropConstraint(tableName string, c *Constraint) (string, error) {
	if c.Name == "" {
		return "", fmt.Errorf("no constraint name provided")
	}
	return "ALTER TABLE " + tableName + " DROP CONSTRAINT " + c.Name + ";\n", nil
}

func AddConstraint(m RemoteMsSql2017, tableName string, c *Constraint, cType string) (string, error) {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " " + strings.ToUpper(cType) + " ("
	for z := 0; z < len(c.Columns); z++ {
		ret += CsColumnToString(m, c.Columns[z]) + ","
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += ");\n"
	return ret, nil
}

func (m RemoteMsSql2017) AddUniqueToString(tableName string, c *Unique) (string, error) {
	return AddConstraint(m, tableName, &c.Constraint, "unique")
}

func (m RemoteMsSql2017) AddPkToString(tableName string, pk *PrimaryKey) (string, error) {
	return AddConstraint(m, tableName, &pk.Constraint, "primary key")
}

func (m RemoteMsSql2017) AddFkToString(tableName string, fk *ForeignKey) (string, error) {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + fk.Name + " FOREIGN KEY ("
	for z := 0; z < len(fk.Columns); z++ {
		ret += CsColumnToString(m, fk.Columns[z])
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += " ) REFERENCES " + fk.Ref_table + " ( "

	for z := 0; z < len(fk.Ref_columns); z++ {
		ret += CsColumnToString(m, fk.Ref_columns[z])
	}
	ret = strings.TrimSuffix(ret, ",")

	ret += " );\n"
	return ret, nil
}

func (m RemoteMsSql2017) AddCheckToString(tableName string, c *Check) (string, error) {
	return "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " CHECK (" + c.Def + ");\n", nil
}

func (m RemoteMsSql2017) DropCsToString(tableName string, c *Constraint) (string, error) {
	if c.Name == "" {
		return "", fmt.Errorf("constraint name was empty")
	}
	return "ALTER TABLE " + tableName + " DROP CONSTRAINT " + c.Name + ";\n", nil
}

func (m RemoteMsSql2017) AddColumnToString(tableName string, c *Column) (string, error) {
	s := c.Name + " "

	cc, e := ColumnTypeToString(c)
	if e != nil {
		return "", e
	}
	s += cc

	if !c.Is_nullable {
		s += " NOT NULL"
	}

	if c.Is_Identity {
		s += " IDENTITY"
	}
	return "ALTER TABLE " + tableName + " ADD " + s + ";\n", nil
}

func (m RemoteMsSql2017) AlterColumnToString(tableName string, c *Column) (string, error) {
	s := c.Name + " "

	cc, e := ColumnTypeToString(c)
	if e != nil {
		return "", e
	}
	s += cc

	if !c.Is_nullable {
		s += " NOT NULL"
	}

	if c.Is_Identity {
		s += " IDENTITY"
	}
	return "ALTER TABLE " + tableName + " ALTER COLUMN " + s + ";\n", nil
}

func (m RemoteMsSql2017) DropColumnToString(tableName string, c *Column) (string, error) {
	return "ALTER TABLE " + tableName + " DROP COLUMN " + c.Name + ";\n", nil
}

func (m RemoteMsSql2017) DropIxToString(tableName string, c *Index) (string, error) {
	return "DROP INDEX " + c.Name + " ON " + tableName + ";\n", nil
}
