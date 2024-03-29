package defs

var selectHavingTests = TableTest{
	name: "select-having",
	Table: tbl(
		"having_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("an_int", fldTypeInt, "min 0", "max 100"),
			srcHdr("an_id_set", fldTypeIDSet),
			srcHdr("an_id", fldTypeID),
			srcHdr("a_string", fldTypeString),
			srcHdr("a_string_set", fldTypeStringSet),
			srcHdr("a_decimal", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(1), int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}, float64(123.45)),
			srcRow(int64(2), int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}, float64(234.56)),
			srcRow(int64(3), int64(33), []int64{31, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}, float64(345.67)),
			srcRow(int64(4), int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}, float64(456.78)),
		),
	),
	SQLTests: []SQLTest{
		{
			name: "select-having",
			SQLs: sqls(
				"select count(*), an_int from having_test group by an_int having count(*) > 0",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("an_int", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(11)),
				row(int64(1), int64(22)),
				row(int64(1), int64(33)),
				row(int64(1), int64(44)),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
	},
}
