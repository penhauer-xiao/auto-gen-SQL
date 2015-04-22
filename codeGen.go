package main

import (
	"database/sql"
	"encoding/xml"
	"fmt"
	_ "github.com/mattn/go-oci8"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"tool"
)

var (
	gSQLModelConf SQLModelConfig
	gDB           *sql.DB
	gInMap        = map[string]map[string]SQLVar{}
	gSliceMap     = map[string]map[string][]SQLVar{}
	gOnlyMap      = map[string]int{}
)

type SQLModelConfig struct {
	Config SQLConfig   `xml:"Config"`
	InPath []string    `xml:"In>Path"`
	InType []string    `xml:"In>Type"`
	Var    SQLVar      `xml:"Var"`
	Action []SQLAction `xml:"Action"`
}

type SQLConfig struct {
	DbUser    string `xml:"DbUser"`
	MakeTable string `xml:"MakeTable"`
}
type SQLVar struct {
	User              string   `xml:"User"`
	CondUser          string   `xml:"CondUser"`
	ActionType        string   `xml:"ActionType"`
	OutType           string   `xml:"OutType"`
	TableCsv          string   `xml:"TableCsv"`
	CurrentTable      string   `xml:"CurrentTabie"`
	HistoryTable      string   `xml:"HistoryTable"`
	HistoryTableList  string   `xml:"HistoryTableList"`
	SelectOutCols     string   `xml:"SelectOutCols"`
	SelectSeqCols     string   `xml:"SelectSeqCols"`
	WhereConds        string   `xml:"WhereConds"`
	SelectKeyCols     string   `xml:"SelectKeyCols"`
	WhereAndCond      string   `xml:"WhereAndCond"`
	CurrentTableSlice []string `xml:"CurrentTableSlice"`
	HistoryTableSlice []string `xml:"HistoryTableSlice"`
	PkName            string   `xml:"PkName"`
	PkCols            string   `xml:"PkiCols"`
	IdxName           string   `xml:"IdxName"`
	IdxCols           string   `xml:"IdxCols"`
	FirstSelectKey    string   `xml:"FirstSelectKey"`
}
type SQLAction struct {
	On             string    `xml:"on,attr"`
	MapField       string    `xml:"mapFled,attr"`
	File           string    `xml:"file,attr"`
	Cond           []SQLCond `xml:"Cond"`
	Replace        []SQLCond `xml:"Replace"`
	Out            []SQLOut  `xml:"Out"`
	Text           []string  `xml:"Text"`
	Rmap           map[string]string
	MapFieldResult map[string]string
}
type SQLCond struct {
	CondVar    string `xml:"condVar,attr"`
	CondChar   string `xml:"condChar,attr"`
	CondValue  string `xml:"condValue,attr"`
	Join       string `xml:"join,attr"`
	Type       string `xml:",chardata"`
	CondVarS   []string
	CondCharS  []string
	CondValueS []string
	JoinS      []string
}
type SQLOut struct {
	Cond    []SQLCond `xml:"Cond"`
	Replace []SQLCond `xml:"Replace"`
	File    []string  `xml:"Filed"`
}

func mutSplit(buf string, key []string) (from, where bool, ret []string) {
	tmp := buf
	from, where = false, false
	for _, v := range key {
		idx := strings.Index(tmp, v)
		if idx != -1 {
			if v == "from " {
				from = true
			}
			if v == "where " {
				where = true
			}
			ret = append(ret, tmp[:idx])
			tmp = tmp[idx+len(v):]
		}
	}
	ret = append(ret, tmp)
	return from, where, ret
}
func getPk(cTable string) (pkname, pkcols string) {
	sqlPk := fmt.Sprintf("select cu.* from user_cons_columns cu, user_constraints au where cu.constraint_name = au.constraint_name and au.constraint_type = 'P' and au.table_name = '%s'", cTable)
	rows, err := gDB.Query(sqlPk)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		name1, name2, name3, name4, name5 := "", "", "", "", ""
		rows.Scan(&name1, &name2, &name3, &name4, &name5)
		pkname = name2
		pkcols += strings.ToLower(name4)
		pkcols += ","
	}
	rows.Close()
	if lIdx := strings.LastIndex(pkcols, ","); lIdx != -1 {
		pkcols = pkcols[:lIdx]
		return pkname, pkcols
	}
	return "", ""
}
func getIdx(cTable string) (idxMap map[string]SQLVar) {
	sqlIdx := fmt.Sprintf("select t.*,i.index_type from user_ind_columns t,user_indexes i where t.index_name = i.index_name and t.table_name = i.table_name and t.table_name = '%s' and i.index_name not in(select cu.constraint_name from user_cons_columns cu, user_constraints au where cu.constraint_name = au.constraint_name and au.constraint_type = 'P' and au.table_name = '%s')", cTable, cTable)
	rows, err := gDB.Query(sqlIdx)
	if err != nil {
		log.Fatal(err)
	}
	tmpMap := map[string]SQLVar{}
	for rows.Next() {
		name1, name2, name3, name4, name5, name6, name7, name8 := "", "", "", "", "", "", "", ""
		rows.Scan(&name1, &name2, &name3, &name4, &name5, &name6, &name7, &name8)
		if tSV, ok := tmpMap[name1]; !ok {
			pkV := SQLVar{"", "", "", "", "", "", "", "", "", "", "", "", "", []string{}, []string{}, "", "", "", "", ""}
			pkV.IdxName = name1
			pkV.IdxCols = strings.ToLower(name4)
			tmpMap[name1] = pkV
		} else {
			tSV.IdxCols += ","
			tSV.IdxCols += strings.ToLower(name6)
			tmpMap[name1] = tSV
		}
	}
	rows.Close()
	return tmpMap
}
func getDBPk(cTable, sliceVar string) string {
	if tabMap, ok := gSliceMap[sliceVar]; !ok {
		if _, ok := tabMap[cTable]; !ok {
			pkSlice := make([]SQLVar, 1)
			pkSlice[0].PkName, pkSlice[0].PkCols = getPk(cTable)
			tmpMap := map[string][]SQLVar{}
			tmpMap[cTable] = pkSlice
			gSliceMap[sliceVar] = tmpMap
			return pkSlice[0].PkCols
		}
	} else if pkS, ok := tabMap[cTable]; !ok {
		pkSlice := make([]SQLVar, 1)
		pkSlice[0].PkName, pkSlice[0].PkCols = getPk(cTable)
		tabMap[cTable] = pkSlice
		gSliceMap[sliceVar] = tabMap
		return pkSlice[0].PkCols
	} else {
		return pkS[0].PkCols
	}
	return ""
}
func getDBIdx(cTable, sliceVar string) {
	if tabMap, ok := gSliceMap[sliceVar]; ok {
		if pKS, ok := tabMap[cTable]; ok && pKS[0].IdxName == "" {
			idxMap := getIdx(cTable)
			for _, v1 := range idxMap {
				if pKS[0].IdxName == "" {
					pKS[0].IdxName = v1.IdxName
					pKS[0].IdxCols = v1.IdxCols
				} else {
					pKS = append(pKS, SQLVar{"", "", "", "", "", "", "", "", "", "", "", "", "", []string{}, []string{}, "", "", v1.IdxName, v1.IdxCols, ""})
				}
			}
			tabMap[cTable] = pKS
			gSliceMap[sliceVar] = tabMap
		}
	}
}
func getDecodeBuf(buf string) string {
	if dIdx := strings.Index(buf, "decode"); dIdx == -1 {
		if gDx1, gDx2 := strings.Index(buf, "("), strings.Index(buf, ","); gDx1 != -1 && gDx2 != -1 {
			buf = buf[gDx1+1 : gDx2]
		}
	}
	return buf
}
func BuildInData() {
	gSQLModelConf.Config.MakeTable = strings.TrimSpace(tool.TrimEnterChar(gSQLModelConf.Config.MakeTable))
	for a, inPath := range gSQLModelConf.InPath {
		fileList := tool.GetFileList(inPath)
		for _, f := range fileList {
			tableCsv := f[strings.LastIndex(f, "/")+1 : strings.Index(f, ".")]
			if gSQLModelConf.Config.MakeTable != "" && strings.Index(gSQLModelConf.Config.MakeTable, tableCsv+".") == -1 {
				continue
			}
			content := strings.ToLower(string(tool.ReadFile(f)))
			if fromC := strings.Count(content, "from "); fromC != 1 {
				log.Printf("\nError:%s\n\n%s\n\n", f, content)
				continue
			}
			_, w, cSlice := mutSplit(content, []string{"select ", "from ", "where ", "order ", "by "})
			l, WhereAndCond, wConds, wIdx := len(cSlice), "", "", -1
			if l < 3 || l > 7 {
				log.Printf("\nError:%s\n\n%s\n\n", f, content)
				continue
			}
			if w && l == 7 {
				wIdx = 3
			} else if w && l == 5 {
				wIdx = 3
			}
			if wIdx != -1 {
				wConds = strings.TrimSpace(cSlice[wIdx])
				if tmpIdx := strings.Index(wConds, ";"); tmpIdx != -1 {
					WhereAndCond = wConds[:tmpIdx] + " and "
					wConds = " where " + wConds[:tmpIdx]
				} else {
					WhereAndCond = wConds + " and "
					wConds = " where " + wConds
				}
			}
			colSeqSlice := strings.FieldsFunc(strings.TrimSpace(cSlice[1]), func(r rune) bool {
				switch r {
				case '|', '\n':
					return true
				}
				return false
			})
			dSlice := []string{}
			lenColSeq := len(colSeqSlice)
			for z, b := range colSeqSlice {
				if tmp := strings.TrimSpace(b); tmp == "'" {
					continue
				} else if (z + 1) == lenColSeq {
					if tmpSlice := strings.Split(tmp, " "); len(tmpSlice) > 0 {
						tmp = strings.TrimSpace(tmpSlice[0])
					}
					dSlice = append(dSlice, getDecodeBuf(tmp))
				} else {
					dSlice = append(dSlice, getDecodeBuf(tmp))
				}
			}
			cTable, HisTable := strings.ToUpper(strings.TrimSpace(cSlice[1])), strings.ToUpper(strings.TrimSpace(cSlice[1]))
			cTableS, HisTableS, firstKey := strings.Split(cTable, ","), strings.Split(HisTable, ","), ""
			tabkeys := getDBPk(tableCsv, "HistoryTableSlice")
			if kdx := strings.Index(tabkeys, ","); kdx != -1 {
				firstKey = tabkeys[:kdx]
			}
			if inMapTable, ok := gInMap[gSQLModelConf.InType[a]]; !ok {
				v := SQLVar{gSQLModelConf.Var.User, gSQLModelConf.Var.CondUser, "", "", tableCsv, cTable, HisTable, HisTable, strings.TrimSpace(cSlice[1]), strings.Join(dSlice, ","), wConds, tabkeys, WhereAndCond, cTableS, HisTableS, "", "", "", "", firstKey}
				inMapTable = map[string]SQLVar{}
				inMapTable[tableCsv] = v
				gInMap[gSQLModelConf.InType[a]] = inMapTable
				continue
			} else if inV, ok := inMapTable[tableCsv]; !ok {
				inV = SQLVar{gSQLModelConf.Var.User, gSQLModelConf.Var.CondUser, "", "", tableCsv, cTable, HisTable, HisTable, strings.TrimSpace(cSlice[1]), strings.Join(dSlice, ","), wConds, tabkeys, WhereAndCond, cTableS, HisTableS, "", "", "", "", firstKey}
				inMapTable[tableCsv] = inV
				gInMap[gSQLModelConf.InType[a]] = inMapTable
			}
		}
	}
}
func BuildPkAndIdx() {
	for _, inTableMap := range gInMap {
		for _, v := range inTableMap {
			for _, cTable := range v.HistoryTableSlice {
				cTable = strings.TrimSpace(cTable)
				getDBPk(cTable, "HistoryTableSlice")
				getDBIdx(cTable, "HistoryTableSlice")
			}
		}
	}
}
func BuildConfigSlice() {
	for a, action := range gSQLModelConf.Action {
		for b, cond := range action.Cond {
			splitChar := ","
			if len(cond.Type) == 1 {
				splitChar = cond.Type
			}
			gSQLModelConf.Action[a].Cond[b].CondVarS = strings.Split(cond.CondVar, splitChar)
			gSQLModelConf.Action[a].Cond[b].CondCharS = strings.Split(cond.CondChar, splitChar)
			gSQLModelConf.Action[a].Cond[b].CondValueS = strings.Split(cond.CondValue, splitChar)
			gSQLModelConf.Action[a].Cond[b].JoinS = strings.Split(cond.Join, splitChar)
			gSQLModelConf.Action[a].Rmap = make(map[string]string)
			gSQLModelConf.Action[a].MapFieldResult = make(map[string]string)
		}
		for c, out := range action.Out {
			for d, cond := range out.Cond {
				splitChar := ","
				if len(cond.Type) == 1 {
					splitChar = cond.Type
				}
				gSQLModelConf.Action[a].Out[c].Cond[d].CondVarS = strings.Split(cond.CondVar, splitChar)
				gSQLModelConf.Action[a].Out[c].Cond[d].CondCharS = strings.Split(cond.CondChar, splitChar)
				gSQLModelConf.Action[a].Out[c].Cond[d].CondValueS = strings.Split(cond.CondValue, splitChar)
				gSQLModelConf.Action[a].Out[c].Cond[d].JoinS = strings.Split(cond.Join, splitChar)
			}
		}
	}
}
func condOk(v SQLVar, condSlice []SQLCond) bool {
	for a, cond := range condSlice {
		for b, condName := range cond.CondVarS {
			typ := reflect.ValueOf(&v).Elem()
			types := typ.Type()
			ok := false
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				if types.Field(i).Name == condName {
					if condSlice[a].Type == "lenint" {
						if cmpLen, err := strconv.Atoi(condSlice[a].CondValueS[b]); err != nil {
							log.Println("Config error", condName, condSlice[a].CondCharS[b], condSlice[a].CondValueS[b], f.Interface().(string))
							break
						} else {
							ok = tool.IntCondAssert(len(f.Interface().(string)), cmpLen, condSlice[a].CondCharS[b])
						}
					} else if condSlice[a].Type == "int" {
						if intV, err := strconv.Atoi(f.Interface().(string)); err != nil {
							log.Println("Config error", condName, condSlice[a].CondCharS[b], condSlice[a].CondValueS[b], f.Interface().(string))
							break
						} else if intV1, err := strconv.Atoi(condSlice[a].CondValueS[b]); err != nil {
							log.Println("Config error", condName, condSlice[a].CondCharS[b], condSlice[a].CondValueS[b], f.Interface().(string))
							break
						} else {
							ok = tool.IntCondAssert(intV, intV1, condSlice[a].CondCharS[b])
						}
					} else if condSlice[a].Type == "string" || len(condSlice[a].Type) == 1 {
						ok = tool.StringCondAssert(f.Interface().(string), condSlice[a].CondValueS[b], condSlice[a].CondCharS[b])
					}
					break
				}
			}
			if ok {
				if b >= len(condSlice[a].JoinS) {
					continue
				}
				if condSlice[a].JoinS[b] == "or" {
					return true
				} else {
					continue
				}
			} else {
				if b >= len(condSlice[a].JoinS) {
					return false
				}
				if condSlice[a].JoinS[b] == "or" {
					continue
				} else {
					return false
				}
			}
		}
	}
	return true
}
func replaceSwitch(v SQLVar, Rmap map[string]string, condReplace []SQLCond, forOut bool) (m map[string]string, v1 SQLVar) {
	for _, r := range condReplace {
		typ := reflect.ValueOf(&v).Elem()
		types := typ.Type()
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			fieldName := types.Field(i).Name
			switch f.Kind() {
			case reflect.Slice:
				if forOut {
					idx := strings.Index(fieldName, "Slice")
					for _, tmpR := range condReplace {
						if fieldName[:idx] == tmpR.CondVar {
							tmpS := []string{}
							for _, s1 := range f.Interface().([]string) {
								tmpS = append(tmpS, strings.Replace(s1, tmpR.CondChar, tmpR.CondValue, -1))
							}
							sVal := reflect.ValueOf(tmpS)
							reflect.ValueOf(&v).Elem().FieldByName(fieldName).Set(sVal)
							break
						}
					}
				}
			case reflect.String:
				if fieldName == r.CondVar {
					if r.Type == "assign" {
						Rmap["["+r.CondVar+"]"] = r.CondValue
					} else if r.Type == "string" {
						Rmap["["+r.CondVar+"]"] = strings.Replace(f.Interface().(string), r.CondChar, r.CondValue, -1)
					}
					break
				}
			}
		}
	}
	return Rmap, v
}
func fieldValueSwitch(fieldName, fieldValue string, condReplace []SQLCond) string {
	for _, v := range condReplace {
		if v.CondVar == fieldName {
			if v.Type == "front" {
				if idx := strings.Index(fieldValue, v.CondChar); idx != -1 {
					return v.CondValue + fieldValue[idx:]
				}
			}
		}
	}
	return fieldValue
}
func reflectSwitch(v SQLVar, buf string) string {
	typ := reflect.ValueOf(&v).Elem()
	types := typ.Type()
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		fieldName := types.Field(i).Name
		switch f.Kind() {
		case reflect.String:
			buf = strings.Replace(buf, "["+fieldName+"]", f.Interface().(string), -1)
		}
	}
	return buf
}
func mapSwitch(Rmap map[string]string, buf string) string {
	for k, v := range Rmap {
		buf = strings.Replace(buf, k, v, -1)
	}
	return buf
}
func relativeIndexMapSwitch(buf, fieldName, cTable string, condReplace []SQLCond, action SQLAction, flag bool) (mapFieldBuf, commBuf string) {
	if idx := strings.Index(fieldName, "Slice"); idx != -1 {
		for _, v := range condReplace {
			if v.CondVar == fieldName[:idx] {
				cTable = strings.Replace(cTable, v.CondValue, v.CondChar, -1)
				break
			}
		}
	}
	if tabMap, ok := gSliceMap[fieldName]; ok {
		if pkS, ok := tabMap[cTable]; ok {
			for _, v := range pkS {
				typ := reflect.ValueOf(&v).Elem()
				types := typ.Type()
				tmpBuf, tmpMapFieldValue, exist := buf, "", false
				for i := 0; i < typ.NumField(); i++ {
					f := typ.Field(i)
					switch f.Kind() {
					case reflect.String:
						if idx := strings.Index(buf, types.Field(i).Name); idx != -1 && f.Interface().(string) != "" {
							fieldValue := fieldValueSwitch(types.Field(i).Name, f.Interface().(string), condReplace)
							tmpBuf = strings.Replace(tmpBuf, "["+types.Field(i).Name+"]", fieldValue, -1)
							exist = true
							if flag && action.MapField == types.Field(i).Name {
								tmpMapFieldValue = fieldValue
							}
						}
					}
				}
				if exist {
					commBuf += tmpBuf
				}
				if tmpMapFieldValue != "" {
					if _, ok := action.MapFieldResult[tmpMapFieldValue]; ok {
						continue
					} else {
						mapFieldBuf += tmpBuf
						action.MapFieldResult[tmpMapFieldValue] = "0"
					}
				}
			}
		}
	}
	return mapFieldBuf, commBuf
}
func relativeIndexPosSwitch(v SQLVar, buf string, condReplace []SQLCond, action SQLAction, flag bool) (mapFieldBuf, commBuf string) {
	mapBufSlice, commBufSlice := []string{}, []string{}
	typ := reflect.ValueOf(&v).Elem()
	types := typ.Type()
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		fieldName := types.Field(i).Name
		switch f.Kind() {
		case reflect.Slice:
			if len(commBufSlice) == 0 {
				if idx := strings.Index(buf, "["+fieldName+"]"); idx != -1 {
					for _, s1 := range f.Interface().([]string) {
						mapFieldBuf, commBuf := relativeIndexMapSwitch(buf, fieldName, strings.TrimSpace(s1), condReplace, action, flag)
						mapFieldBuf = strings.Replace(mapFieldBuf, "["+fieldName+"]", strings.TrimSpace(s1), -1)
						commBuf = strings.Replace(commBuf, "["+fieldName+"]", strings.TrimSpace(s1), -1)
						mapBufSlice, commBufSlice = append(mapBufSlice, mapFieldBuf), append(commBufSlice, commBuf)
					}
				}
			} else {
				for idx, s1 := range f.Interface().([]string) {
					mapBufSlice[idx] = strings.Replace(mapBufSlice[idx], "["+fieldName+"]", strings.TrimSpace(s1), -1)
					commBufSlice[idx] = strings.Replace(commBufSlice[idx], "["+fieldName+"]", strings.TrimSpace(s1), -1)
				}
			}
		}
	}
	if len(commBufSlice) == 0 {
		return buf, buf
	}
	return strings.Join(mapBufSlice, "\n"), strings.Join(commBufSlice, "\n")
}
func gen(outType string, v SQLVar, action SQLAction, flag bool) string {
	tmpBuf := ""
	if action.On == "N" {
		return tmpBuf
	}
	if ok := condOk(v, action.Cond); !ok {
		return tmpBuf
	}
	v.OutType = outType
	action.Rmap, v = replaceSwitch(v, action.Rmap, action.Replace, false)
	for _, out := range action.Out {
		if ok := condOk(v, out.Cond); !ok {
			continue
		}
		Rmap1, v1 := replaceSwitch(v, action.Rmap, out.Replace, true)
		for _, f := range out.File {
			f = mapSwitch(Rmap1, f)
			f = reflectSwitch(v1, f)
			mapFieldBufAll, commBufAll := "", ""
			for _, buf := range action.Text {
				buf = mapSwitch(Rmap1, buf)
				mapFieldBuf, commBuf := relativeIndexPosSwitch(v1, buf, out.Replace, action, flag)
				mapFieldBuf, commBuf = reflectSwitch(v1, mapFieldBuf), reflectSwitch(v1, commBuf)
				mapFieldBufAll += mapFieldBuf
				commBufAll += commBuf
			}
			tmpBuf += mapFieldBufAll
			os.MkdirAll(f[:strings.LastIndex(f, "/")], 0755)
			f, err := os.OpenFile(f, os.O_WRONLY|os.O_CREATE, os.ModePerm)
			defer f.Close()
			checkerr(err)
			_, err = f.WriteString(commBufAll)
			checkerr(err)
		}
	}
	return tmpBuf
}

func InputCofnigGen() {
	for outType, tableMap := range gInMap {
		for _, v := range tableMap {
			for _, action := range gSQLModelConf.Action {
				gen(outType, v, action, false)
			}
		}
	}
}
func CofnigInputGen() {
	for _, action := range gSQLModelConf.Action {
		if action.File == "" {
			continue
		}
		allText := ""
		for outType, tableMap := range gInMap {
			for _, v := range tableMap {
				allText += gen(outType, v, action, true)
			}
		}
		os.MkdirAll(action.File[:strings.LastIndex(action.File, "/")], 0755)
		f, err := os.OpenFile(action.File, os.O_WRONLY|os.O_CREATE, os.ModePerm)
		defer f.Close()
		checkerr(err)
		_, err = f.WriteString(tool.TrimEnterChar(allText))
		checkerr(err)
		tool.ClearMapSS(action.MapFieldResult)
	}
}
func main() {
	f, err := os.OpenFile("error.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	defer f.Close()
	if err != nil {
		log.Println(err)
		return
	}
	log.SetOutput(f)
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	os.Setenv("NLS_LANG", "")
	if xmlbuf, xmlerr := ioutil.ReadFile("Model/Model.xml"); xmlerr != nil {
		log.Fatal("read xml fail:", xmlerr)
		return
	} else if xmlerr = xml.Unmarshal(xmlbuf, &gSQLModelConf); xmlerr != nil {
		log.Fatal("Unmarshal xml fail:", xmlerr)
		return
	}
	gDB, err = sql.Open("oci8", gSQLModelConf.Config.DbUser)
	if err != nil {
		log.Fatal(err)
	}
	defer gDB.Close()
	BuildInData()
	BuildPkAndIdx()
	BuildConfigSlice()
	InputCofnigGen()
	CofnigInputGen()
}
func checkerr(err error) {
	if err != nil {
		log.Print("checkerr: panic:%s\n", err.Error())
		panic(err.Error())
	}
}
