package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"diagonal.works/b6"
	"diagonal.works/b6/ingest"
	"diagonal.works/b6/ingest/compact"
	"diagonal.works/b6/ingest/gdal"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

type AgeRange struct {
	Begin int
	End   int // Exclusive
}

func (a AgeRange) Contains(age int) bool {
	return age >= a.Begin && (age < a.End || a.End == 0)
}

type AgePrevalence struct {
	AgeRange   AgeRange
	Prevalence float64
}

func (a AgePrevalence) String() string {
	if a.AgeRange.End > 0 {
		return fmt.Sprintf("%d-%d: %f", a.AgeRange.Begin, a.AgeRange.End, a.Prevalence)
	} else {
		return fmt.Sprintf("%d+: %f", a.AgeRange.Begin, a.Prevalence)
	}
}

type AgePrevalences [][]AgePrevalence

func (a AgePrevalences) Prevalence(sex Sex, age int) float64 {
	for _, p := range a[sex] {
		if p.AgeRange.Contains(age) {
			return p.Prevalence
		}
	}
	return 0.0
}

func (a AgePrevalences) Log() {
	for sex, ranges := range a {
		log.Printf("%s", Sex(sex))
		for _, p := range ranges {
			log.Printf("  %s", p)
		}
	}
}

// Copied from Health Survey for England 2019, Adult health table 2
var DiabetesPrevalence = AgePrevalences{
	// Male
	{
		{AgeRange: AgeRange{Begin: 16, End: 25}, Prevalence: 1.0 / 100.0},
		{AgeRange: AgeRange{Begin: 25, End: 35}, Prevalence: 1.0 / 100.0},
		{AgeRange: AgeRange{Begin: 35, End: 45}, Prevalence: 3.0 / 100.0},
		{AgeRange: AgeRange{Begin: 45, End: 55}, Prevalence: 9.0 / 100.0},
		{AgeRange: AgeRange{Begin: 55, End: 65}, Prevalence: 13.0 / 100.0},
		{AgeRange: AgeRange{Begin: 65, End: 75}, Prevalence: 21.0 / 100.0},
		{AgeRange: AgeRange{Begin: 75}, Prevalence: 18.0 / 100.0},
	},
	// Female
	{
		{AgeRange: AgeRange{Begin: 16, End: 25}, Prevalence: 0.0 / 100.0},
		{AgeRange: AgeRange{Begin: 25, End: 35}, Prevalence: 1.0 / 100.0},
		{AgeRange: AgeRange{Begin: 35, End: 45}, Prevalence: 3.0 / 100.0},
		{AgeRange: AgeRange{Begin: 45, End: 55}, Prevalence: 4.0 / 100.0},
		{AgeRange: AgeRange{Begin: 55, End: 65}, Prevalence: 9.0 / 100.0},
		{AgeRange: AgeRange{Begin: 65, End: 75}, Prevalence: 15.0 / 100.0},
		{AgeRange: AgeRange{Begin: 75}, Prevalence: 6.0 / 100.0},
	},
}

// Copied from Health Survey for England 2019, Adult health table 2
var HypertensionPrevalence = AgePrevalences{
	// Male
	{
		{AgeRange: AgeRange{Begin: 16, End: 25}, Prevalence: 9.0 / 100.0},
		{AgeRange: AgeRange{Begin: 25, End: 35}, Prevalence: 8.0 / 100.0},
		{AgeRange: AgeRange{Begin: 35, End: 45}, Prevalence: 13.0 / 100.0},
		{AgeRange: AgeRange{Begin: 45, End: 55}, Prevalence: 29.0 / 100.0},
		{AgeRange: AgeRange{Begin: 55, End: 65}, Prevalence: 48.0 / 100.0},
		{AgeRange: AgeRange{Begin: 65, End: 75}, Prevalence: 58.0 / 100.0},
		{AgeRange: AgeRange{Begin: 75}, Prevalence: 68.0 / 100.0},
	},
	// Female
	{
		{AgeRange: AgeRange{Begin: 16, End: 25}, Prevalence: 1.0 / 100.0},
		{AgeRange: AgeRange{Begin: 25, End: 35}, Prevalence: 6.0 / 100.0},
		{AgeRange: AgeRange{Begin: 35, End: 45}, Prevalence: 9.0 / 100.0},
		{AgeRange: AgeRange{Begin: 45, End: 55}, Prevalence: 22.0 / 100.0},
		{AgeRange: AgeRange{Begin: 55, End: 65}, Prevalence: 33.0 / 100.0},
		{AgeRange: AgeRange{Begin: 65, End: 75}, Prevalence: 51.0 / 100.0},
		{AgeRange: AgeRange{Begin: 75}, Prevalence: 65.0 / 100.0},
	},
}

// Copied from PHE's COPD prevalence model for small populations, quoting
// data from Nacul et al
// https://fingertips.phe.org.uk/documents/COPD-prevalence-model-Technical-Document-v1.2-0fc%20(1).docx
var COPDPrevalence = AgePrevalences{
	// Male
	{
		{AgeRange: AgeRange{Begin: 15, End: 45}, Prevalence: 1.30 / 100.0},
		{AgeRange: AgeRange{Begin: 45, End: 55}, Prevalence: 2.38 / 100.0},
		{AgeRange: AgeRange{Begin: 55, End: 65}, Prevalence: 6.90 / 100.0},
		{AgeRange: AgeRange{Begin: 65, End: 75}, Prevalence: 10.03 / 100.0},
		{AgeRange: AgeRange{Begin: 75}, Prevalence: 11.65 / 100.0},
	},
	// Female
	{
		{AgeRange: AgeRange{Begin: 15, End: 45}, Prevalence: 0.89 / 100.0},
		{AgeRange: AgeRange{Begin: 45, End: 55}, Prevalence: 2.00 / 100.0},
		{AgeRange: AgeRange{Begin: 55, End: 65}, Prevalence: 4.11 / 100.0},
		{AgeRange: AgeRange{Begin: 65, End: 75}, Prevalence: 4.81 / 100.0},
		{AgeRange: AgeRange{Begin: 75}, Prevalence: 5.55 / 100.0},
	},
}

type Prevalences map[QOFCondition]AgePrevalences

var AllPrevalences = Prevalences{
	QOFConditionDiabetes:     DiabetesPrevalence,
	QOFConditionCOPD:         COPDPrevalence,
	QOFConditionHypertension: HypertensionPrevalence,
}

// Comorbidities are from https://multimorbidity.caliberresearch.org/CFA
var HypertensionAndCOPDPrevalence = AgePrevalences{
	// Data isn't broken down by sex in the interactive tool, so we duplicate
	// the average data here. The breakdown is available in their CSVs, though
	// is also broken down by race, so we'd have to standardise it.
	{
		{AgeRange: AgeRange{Begin: 30, End: 39}, Prevalence: 0.050494 / 100.0},
		{AgeRange: AgeRange{Begin: 40, End: 49}, Prevalence: 0.336516 / 100.0},
		{AgeRange: AgeRange{Begin: 50, End: 59}, Prevalence: 1.561707 / 100.0},
		{AgeRange: AgeRange{Begin: 60, End: 69}, Prevalence: 4.829698 / 100.0},
		{AgeRange: AgeRange{Begin: 70}, Prevalence: 9.684548 / 100.0},
	},
	{
		{AgeRange: AgeRange{Begin: 30, End: 39}, Prevalence: 0.050494 / 100.0},
		{AgeRange: AgeRange{Begin: 40, End: 49}, Prevalence: 0.336516 / 100.0},
		{AgeRange: AgeRange{Begin: 50, End: 59}, Prevalence: 1.561707 / 100.0},
		{AgeRange: AgeRange{Begin: 60, End: 69}, Prevalence: 4.829698 / 100.0},
		{AgeRange: AgeRange{Begin: 70}, Prevalence: 9.684548 / 100.0},
	},
}

var DiabetesAndCOPDPrevalence = AgePrevalences{
	{
		{AgeRange: AgeRange{Begin: 30, End: 39}, Prevalence: 0.020673 / 100.0},
		{AgeRange: AgeRange{Begin: 40, End: 49}, Prevalence: 0.136145 / 100.0},
		{AgeRange: AgeRange{Begin: 50, End: 59}, Prevalence: 0.573859 / 100.0},
		{AgeRange: AgeRange{Begin: 60, End: 69}, Prevalence: 1.612181 / 100.0},
		{AgeRange: AgeRange{Begin: 70}, Prevalence: 3.162693 / 100.0},
	},
	{
		{AgeRange: AgeRange{Begin: 30, End: 39}, Prevalence: 0.020673 / 100.0},
		{AgeRange: AgeRange{Begin: 40, End: 49}, Prevalence: 0.136145 / 100.0},
		{AgeRange: AgeRange{Begin: 50, End: 59}, Prevalence: 0.573859 / 100.0},
		{AgeRange: AgeRange{Begin: 60, End: 69}, Prevalence: 1.612181 / 100.0},
		{AgeRange: AgeRange{Begin: 70}, Prevalence: 3.162693 / 100.0},
	},
}

var DiabetesAndHypertensionPrevalence = AgePrevalences{
	{
		{AgeRange: AgeRange{Begin: 20, End: 29}, Prevalence: 0.051430 / 100.0},
		{AgeRange: AgeRange{Begin: 30, End: 39}, Prevalence: 0.384745 / 100.0},
		{AgeRange: AgeRange{Begin: 40, End: 49}, Prevalence: 1.889034 / 100.0},
		{AgeRange: AgeRange{Begin: 50, End: 59}, Prevalence: 5.351880 / 100.0},
		{AgeRange: AgeRange{Begin: 60, End: 69}, Prevalence: 10.156242 / 100.0},
		{AgeRange: AgeRange{Begin: 70, End: 79}, Prevalence: 16.385307 / 100.0},
		{AgeRange: AgeRange{Begin: 80}, Prevalence: 15.076283 / 100.0},
	},
	{
		{AgeRange: AgeRange{Begin: 20, End: 29}, Prevalence: 0.051430 / 100.0},
		{AgeRange: AgeRange{Begin: 30, End: 39}, Prevalence: 0.384745 / 100.0},
		{AgeRange: AgeRange{Begin: 40, End: 49}, Prevalence: 1.889034 / 100.0},
		{AgeRange: AgeRange{Begin: 50, End: 59}, Prevalence: 5.351880 / 100.0},
		{AgeRange: AgeRange{Begin: 60, End: 69}, Prevalence: 10.156242 / 100.0},
		{AgeRange: AgeRange{Begin: 70, End: 79}, Prevalence: 16.385307 / 100.0},
		{AgeRange: AgeRange{Begin: 80}, Prevalence: 15.076283 / 100.0},
	},
}

type Comorbidities map[[2]QOFCondition]AgePrevalences

var AllComorbidities = Comorbidities{
	{QOFConditionHypertension, QOFConditionCOPD}:     HypertensionAndCOPDPrevalence,
	{QOFConditionDiabetes, QOFConditionCOPD}:         DiabetesAndCOPDPrevalence,
	{QOFConditionDiabetes, QOFConditionHypertension}: DiabetesAndHypertensionPrevalence,

	{QOFConditionCOPD, QOFConditionHypertension}:     HypertensionAndCOPDPrevalence,
	{QOFConditionCOPD, QOFConditionDiabetes}:         DiabetesAndCOPDPrevalence,
	{QOFConditionHypertension, QOFConditionDiabetes}: DiabetesAndHypertensionPrevalence,
}

type JointCondition struct {
	Conditions    [2]QOFCondition
	SecondPresent bool
}

type JointPrevalences map[JointCondition]AgePrevalences

// Incredibly rough, and wrong, estimates for the average
// number of appointments by age. Derived in a hacky spreadsheet from
// the Health Survey England data, which only buckets appointments
// into, 0, 1-2, or 3+ by estimating the average of the 3+ bucket assuming
// a 1.5 average for the 1-2 bucket, given a national appointments/people
// ratio of 4.75. To seed the visualisation only, rather than for any
// actual analysis.
var AppointmentsNoConditions = AgePrevalences{
	{
		{AgeRange: AgeRange{Begin: 0, End: 15}, Prevalence: 3.5},
		{AgeRange: AgeRange{Begin: 16, End: 24}, Prevalence: 3.5},
		{AgeRange: AgeRange{Begin: 25, End: 34}, Prevalence: 4.0},
		{AgeRange: AgeRange{Begin: 35, End: 44}, Prevalence: 4.0},
		{AgeRange: AgeRange{Begin: 45, End: 54}, Prevalence: 5.25},
		{AgeRange: AgeRange{Begin: 55, End: 64}, Prevalence: 5.0},
		{AgeRange: AgeRange{Begin: 65, End: 74}, Prevalence: 5.5},
		{AgeRange: AgeRange{Begin: 75}, Prevalence: 6.0},
	},
}

var AppointmentsOneCondition = AgePrevalences{
	{
		{AgeRange: AgeRange{Begin: 0, End: 15}, Prevalence: 6.0},
		{AgeRange: AgeRange{Begin: 16, End: 24}, Prevalence: 6.0},
		{AgeRange: AgeRange{Begin: 25, End: 34}, Prevalence: 7.0},
		{AgeRange: AgeRange{Begin: 35, End: 44}, Prevalence: 6.5},
		{AgeRange: AgeRange{Begin: 45, End: 54}, Prevalence: 9.0},
		{AgeRange: AgeRange{Begin: 55, End: 64}, Prevalence: 8.5},
		{AgeRange: AgeRange{Begin: 65, End: 74}, Prevalence: 9.5},
		{AgeRange: AgeRange{Begin: 75}, Prevalence: 10.0},
	},
}

var AppointmentsTwoConditions = AgePrevalences{
	{
		{AgeRange: AgeRange{Begin: 0, End: 15}, Prevalence: 12.0},
		{AgeRange: AgeRange{Begin: 16, End: 24}, Prevalence: 12.0},
		{AgeRange: AgeRange{Begin: 25, End: 34}, Prevalence: 13.5},
		{AgeRange: AgeRange{Begin: 35, End: 44}, Prevalence: 13.0},
		{AgeRange: AgeRange{Begin: 45, End: 54}, Prevalence: 18.5},
		{AgeRange: AgeRange{Begin: 55, End: 64}, Prevalence: 17.0},
		{AgeRange: AgeRange{Begin: 65, End: 74}, Prevalence: 19.0},
		{AgeRange: AgeRange{Begin: 75}, Prevalence: 20.5},
	},
}

var AppointmentsThreeConditions = AgePrevalences{
	{
		{AgeRange: AgeRange{Begin: 0, End: 15}, Prevalence: 12.0},
		{AgeRange: AgeRange{Begin: 16, End: 24}, Prevalence: 12.0},
		{AgeRange: AgeRange{Begin: 25, End: 34}, Prevalence: 13.5},
		{AgeRange: AgeRange{Begin: 35, End: 44}, Prevalence: 13.0},
		{AgeRange: AgeRange{Begin: 45, End: 54}, Prevalence: 18.5},
		{AgeRange: AgeRange{Begin: 55, End: 64}, Prevalence: 17.0},
		{AgeRange: AgeRange{Begin: 65, End: 74}, Prevalence: 19.0},
		{AgeRange: AgeRange{Begin: 75}, Prevalence: 20.5},
	},
}

type ICBCode string

func (i ICBCode) String() string {
	return string(i)
}

type GPPracticeCode string

func (g GPPracticeCode) String() string {
	return string(g)
}

type GPPracticeCodeSet map[GPPracticeCode]struct{}

const (
	GPPracticeCodeInvalid GPPracticeCode = ""
)

const (
	ICBDataLSOACodeColumn = "LSOA11CD"
	ICBDataICBCodeColumn  = "ICB22CDH"
	ICBDataICBNameColumn  = "ICB22NM"

	LSOADataLSOACodeColumn   = "LSOA Code"
	LSOADataLSOANameColumn   = "LSOA Name"
	LSOADataAllAgesColumn    = "All Ages"
	LSOADataNinetyPlusColumn = "90+"

	LSOADataMaxAge = 90

	GPPracticeDataCodeColumn     = 0
	GPPracticeDataNameColumn     = 1
	GPPracticeDataICBCodeColumn  = 3
	GPPracticeDataPostcodeColumn = 9
	GPPracticeDataStatusColumn   = 12

	GPPractionerDataPracticeCodeColumn = 14

	GPQOFDataPracticeCodeColumn = "Practice code"
	GPQOFDataListSizeColumn     = "List size"
	GPQOFDataPrevalenceColumn   = "Prevalence (%)"

	GPAppointmentsCodeColumn       = "GP_CODE"
	GPAppointmentsHcpTypeColumn    = "HCP_TYPE"
	GPAppointmentsStatusColumn     = "APPT_STATUS"
	GPAppointmentsNationalCategory = "NATIONAL_CATEGORY"
	GPAppointmentsCountColumn      = "COUNT_OF_APPOINTMENTS"

	GPAppointmentsStatusAttended = "Attended"

	TrustSiteCodeColumn       = 0
	TrustSiteNameColumn       = 1
	TrustSiteAddressOneColumn = 4
	TrustSitePostcodeColumn   = 9

	EstatesSiteCodeColumn = "Site Code"
	EstatesSiteTypeColumn = "Site Type"

	LSOAToMSOALSOACodeColumn = "LSOA11CD"
	LSOAToMSOAMSOACodeColumn = "MSOA11CD"
	LSOAToMSOAMSOANameColumn = "MSOA11NM"

	IMDLSOACodeColumn   = "LSOA code (2011)"
	IMDLSOAScoreColumn  = "Index of Multiple Deprivation (IMD) Score"
	IMDLSOADecileColumn = "Index of Multiple Deprivation (IMD) Decile (where 1 is most deprived 10% of LSOAs)"

	NorthCentralLondonICBCode = ICBCode("QMJ")
	Camden007FLSOACode        = LSOACode("E01000927")
)

const (
	// The radius from a GP surgery in meters from which we'll draw
	// patients
	GPLSOANearbyRadiusM = 3000.0

	// Some practices have prevalences high enough to suggest that they're
	// not reporting correctly, so replace these with the average
	// TODO: Consider using a number of standard deviations or similar.
	QPQOFDataPrevalenceOutlier = 0.40
)

type GPPracticeStatus string

func (g GPPracticeStatus) String() string {
	return string(g)
}

const (
	GPPracticeStatusActive   GPPracticeStatus = "A"
	GPPracticeStatusClosed   GPPracticeStatus = "C"
	GPPracticeStatusDormant  GPPracticeStatus = "D"
	GPPracticeStatusProposed GPPracticeStatus = "P"
)

type QOFCondition int

const (
	QOFConditionDiabetes QOFCondition = iota
	QOFConditionHypertension
	QOFConditionCOPD

	QOFConditionLast                 = QOFConditionCOPD
	QOFConditionInvalid QOFCondition = -1
)

func (q QOFCondition) String() string {
	switch q {
	case QOFConditionDiabetes:
		return "dm"
	case QOFConditionHypertension:
		return "hyp"
	case QOFConditionCOPD:
		return "copd"
	}
	return "invalid"
}

func QOFConditionFromString(s string) QOFCondition {
	for i := QOFCondition(0); i <= QOFConditionLast; i++ {
		if s == i.String() {
			return i
		}
	}
	return QOFConditionInvalid
}

type HcpType int

const (
	HcpTypeGP HcpType = iota
	HcpTypeOther
	HcpTypeUnknown

	HcpTypeLast            = HcpTypeUnknown
	HcpTypeInvalid HcpType = -1
)

func (q HcpType) String() string {
	switch q {
	case HcpTypeGP:
		return "gp"
	case HcpTypeOther:
		return "other"
	case HcpTypeUnknown:
		return "unknown"
	}
	return "invalid"
}

func HcpTypeFromString(s string) HcpType {
	switch s {
	case "GP":
		return HcpTypeGP
	case "Other Practice staff":
		return HcpTypeOther
	}
	return HcpTypeUnknown
}

type ICB struct {
	Name  string
	LSOAs LSOASet
}

type MSOACode string

func (m MSOACode) String() string {
	return string(m)
}

type MSOA struct {
	Code MSOACode
	Name string
}

type LSOACode string

func (l LSOACode) String() string {
	return string(l)
}

type LSOASet map[LSOACode]struct{}

type LSOA struct {
	Code         LSOACode
	MSOACode     MSOACode
	Name         string
	Center       s2.Point
	PersonsByAge []int
	MalesByAge   []int
	FemalesByAge []int
	IMD          float64
	IMDDecile    int
}

type ConditionFraction [QOFConditionLast + 1]float64

func (c ConditionFraction) String() string {
	parts := make([]string, 0, len(c))
	var condition QOFCondition
	for condition = 0; condition <= QOFConditionLast; condition++ {
		parts = append(parts, fmt.Sprintf("%s: %.02f", condition, c[condition]))
	}
	return strings.Join(parts, " ")
}

type GPPractice struct {
	Code                GPPracticeCode
	Name                string
	ICB                 ICBCode
	Status              GPPracticeStatus
	Practioners         int
	Postcode            string
	Location            s2.Point
	LSOA                LSOACode
	ListSize            int
	ConditionPrevalence [QOFConditionLast + 1]float64
	ConditionBias       [QOFConditionLast + 1]float64
	Appointments        int
	AppointmentsByType  [HcpTypeLast + 1]int

	SimulatedListSize        int
	SimulatedConditionCounts [QOFConditionLast + 1]int
}

func readICBs() (map[ICBCode]*ICB, error) {
	f, err := os.Open("data/lsoa-icb.csv.gz")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	g, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}

	r := csv.NewReader(g)
	r.Comment = '#'
	r.FieldsPerRecord = -1

	icbs := make(map[ICBCode]*ICB)
	body := false
	columns := make(map[string]int)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return icbs, err
		}
		if len(row) > 0 {
			if !body && row[0] == ICBDataLSOACodeColumn {
				for i, header := range row {
					columns[header] = i
				}
				body = true
			} else if body {
				code := ICBCode(row[columns[ICBDataICBCodeColumn]])
				icb, ok := icbs[code]
				if !ok {
					icb = &ICB{Name: row[columns[ICBDataICBNameColumn]], LSOAs: make(LSOASet)}
					icbs[code] = icb
				}
				lsoa := LSOACode(row[columns[ICBDataLSOACodeColumn]])
				icb.LSOAs[lsoa] = struct{}{}
			}
		}
	}
	return icbs, nil
}

func parseAgeHeaders(row []string) ([]int, error) {
	columns := make([]int, LSOADataMaxAge+1)
	ages := false
	for i, header := range row {
		if !ages {
			if header == LSOADataAllAgesColumn {
				ages = true
			}
		} else if ages {
			if header == LSOADataNinetyPlusColumn {
				columns[LSOADataMaxAge] = i
				break
			} else {
				age, err := strconv.Atoi(header)
				if err != nil {
					return columns, fmt.Errorf("bad age header %q", header)
				}
				columns[age] = i
			}
		}
	}
	return columns, nil
}

// readByAge reads populations counts that have been broken down by age,
// as the male/female/persons files have the same format
func readByAge(filename string, emit func(LSOACode, string, []int) error) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	g, err := gzip.NewReader(f)
	if err != nil {
		return err
	}

	r := csv.NewReader(g)
	r.Comment = '#'
	r.FieldsPerRecord = -1
	body := false
	var ageColumns []int
	nameColumn := -1
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if len(row) > 0 {
			if !body && row[0] == LSOADataLSOACodeColumn {
				ageColumns, err = parseAgeHeaders(row)
				if err != nil {
					return err
				}
				for i, column := range row {
					if column == LSOADataLSOANameColumn {
						nameColumn = i
						break
					}
				}
				body = true
			} else if body {
				counts := make([]int, LSOADataMaxAge+1)
				for age := range counts {
					count, err := strconv.Atoi(strings.Replace(row[ageColumns[age]], ",", "", -1))
					if err != nil {
						return fmt.Errorf("bad age count %q", row[ageColumns[age]])
					}
					counts[age] = count
				}
				if err := emit(LSOACode(row[0]), row[nameColumn], counts); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func readLSOAs(w b6.World) (map[LSOACode]*LSOA, error) {
	lsoas := make(map[LSOACode]*LSOA)
	emit := func(code LSOACode, name string, counts []int) error {
		lsoas[code] = &LSOA{Code: code, Name: name, PersonsByAge: counts}
		return nil
	}
	if err := readByAge("data/lsoa-persons.csv.gz", emit); err != nil {
		return nil, err
	}
	emit = func(code LSOACode, name string, counts []int) error {
		lsoas[code].MalesByAge = counts
		return nil
	}
	if err := readByAge("data/lsoa-males.csv.gz", emit); err != nil {
		return nil, err
	}
	emit = func(code LSOACode, name string, counts []int) error {
		lsoas[code].FemalesByAge = counts
		return nil
	}
	if err := readByAge("data/lsoa-females.csv.gz", emit); err != nil {
		return nil, err
	}
	for _, lsoa := range lsoas {
		id := b6.FeatureIDFromUKONSCode(lsoa.Code.String(), 2011, b6.FeatureTypeArea)
		if f := b6.FindAreaByID(id.ToAreaID(), w); f != nil {
			lsoa.Center = b6.Centroid(f)
		} else {
			return nil, fmt.Errorf("No LSOA boundary for %s", lsoa.Code)
		}
	}
	return lsoas, nil
}

func fillMSOAs(lsoas map[LSOACode]*LSOA) (map[MSOACode]*MSOA, error) {
	f, err := os.Open("data/lsoa-msoa.csv.gz")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	g, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}

	r := csv.NewReader(g)
	r.Comment = '#'

	msoas := make(map[MSOACode]*MSOA)
	columns := make(map[string]int)
	row, err := r.Read()
	if err != nil {
		return nil, err
	}
	for i, column := range row {
		columns[column] = i
	}

	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		msoa := MSOACode(row[columns[LSOAToMSOAMSOACodeColumn]])
		if _, ok := msoas[msoa]; !ok {
			msoas[msoa] = &MSOA{
				Code: msoa,
				Name: row[columns[LSOAToMSOAMSOANameColumn]],
			}
		}
		lsoa := LSOACode(row[columns[LSOAToMSOALSOACodeColumn]])
		if _, ok := lsoas[lsoa]; ok {
			lsoas[lsoa].MSOACode = msoa
		}
	}
	return msoas, nil
}

func fillIMDs(lsoas map[LSOACode]*LSOA) error {
	f, err := os.Open("data/lsoa-imd.csv.gz")
	if err != nil {
		return err
	}
	defer f.Close()

	g, err := gzip.NewReader(f)
	if err != nil {
		return err
	}

	r := csv.NewReader(g)
	r.Comment = '#'

	columns := make(map[string]int)
	row, err := r.Read()
	if err != nil {
		return err
	}
	for i, column := range row {
		columns[column] = i
	}

	badLSOA := 0
	badScore := 0
	badDecile := 0
	n := 0
	total := 0.0
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		code := LSOACode(row[columns[IMDLSOACodeColumn]])
		if lsoa, ok := lsoas[code]; ok {
			if score, err := parseFloat(row[columns[IMDLSOAScoreColumn]]); err == nil {
				lsoa.IMD = score
				total += score
			} else {
				badScore++
			}
			if decile, err := strconv.Atoi(row[columns[IMDLSOADecileColumn]]); err == nil {
				lsoa.IMDDecile = decile
			} else {
				badDecile++
			}
			n++
		} else {
			badLSOA++
		}
	}
	log.Printf("imd: bad lsoa: %d bad score: %d bad decile: %d imd average: %f", badLSOA, badScore, badDecile, total/float64(n))
	return nil
}

func readGPPracticeListSizes(gps map[GPPracticeCode]*GPPractice) error {
	f, err := os.Open("data/qof-condition/af.csv.gz")
	if err != nil {
		return err
	}
	defer f.Close()

	g, err := gzip.NewReader(f)
	if err != nil {
		return err
	}

	r := csv.NewReader(g)
	r.Comment = '#'
	r.FieldsPerRecord = -1
	code := -1
	listSize := -1
	missingGPs := 0
	badListSize := 0
	totalListSize := 0
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if code < 0 {
			for i, col := range row {
				switch col {
				case GPQOFDataPracticeCodeColumn:
					code = i
				case GPQOFDataListSizeColumn:
					if listSize < 0 { // Second occurance is year-on-year change
						listSize = i
					}
				}
			}
		} else if listSize > 0 {
			if gp, ok := gps[GPPracticeCode(row[code])]; ok {
				var err error
				if gp.ListSize, err = strconv.Atoi(strings.Replace(strings.TrimSpace(row[listSize]), ",", "", -1)); err == nil {
					totalListSize += gp.ListSize
				} else {
					badListSize++
				}
			} else {
				missingGPs++
			}
		}
	}
	log.Printf("list size assignment:")
	log.Printf("  bad list size: %d", badListSize)
	log.Printf("  missing gps: %d", missingGPs)
	log.Printf("  total list size: %d", totalListSize)
	return nil
}

func readGPPracticeConditionPrevalence(gps map[GPPracticeCode]*GPPractice, conditions []QOFCondition) error {
	badPrevalence := 0
	missingGPs := 0
	outlierGPs := 0
	var average ConditionFraction
	var coverage ConditionFraction
	for _, condition := range conditions {
		outliers := make([]*GPPractice, 0)
		f, err := os.Open(fmt.Sprintf("data/qof-condition/%s.csv.gz", condition.String()))
		if err != nil {
			return err
		}
		defer f.Close()

		g, err := gzip.NewReader(f)
		if err != nil {
			return err
		}

		r := csv.NewReader(g)
		r.Comment = '#'
		r.FieldsPerRecord = -1
		code := -1
		prevalence := -1
		n := 0
		for {
			row, err := r.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}
			if code < 0 {
				for i, col := range row {
					switch col {
					case GPQOFDataPracticeCodeColumn:
						code = i
					case GPQOFDataPrevalenceColumn:
						if prevalence < 0 { // Second occurance is year-on-year change
							prevalence = i
						}
					}
				}
			} else if prevalence > 0 {
				if gp, ok := gps[GPPracticeCode(row[code])]; ok {
					coverage[condition]++
					if p, err := parseFloat(row[prevalence]); err == nil {
						gp.ConditionPrevalence[condition] = p / 100.0
						if p/100.0 < QPQOFDataPrevalenceOutlier {
							average[condition] += (p / 100.0)
							n++
						} else {
							outliers = append(outliers, gp)
						}
					} else {
						badPrevalence++
					}
				} else {
					missingGPs++
				}
			}
		}
		if n > 0 {
			average[condition] /= float64(n)
			for _, gp := range outliers {
				gp.ConditionPrevalence[condition] = average[condition]
				outlierGPs++
			}
		}
	}
	log.Printf("prevalence assignment:")
	log.Printf("  bad prevalence: %d", badPrevalence)
	log.Printf("  missing gps: %d", missingGPs)
	log.Printf("  outlying gps * conditions: %d", outlierGPs)
	log.Printf("  average prevalence: %s", average.String())
	log.Printf("  coverage:")
	for _, condition := range conditions {
		log.Printf("    %s: %.02f", condition, coverage[condition]/float64(len(gps)))
	}
	return nil
}

func imputeMissingPrevalanceFromNearby(gps map[GPPracticeCode]*GPPractice, conditions []QOFCondition, nearby map[LSOACode][]GPPracticeCode) {
	log.Printf("imputeMissingPrevalanceFromNearby")
	missing := 0
	imputed := 0
	for _, gp := range gps {
		for _, condition := range conditions {
			if gp.ConditionPrevalence[condition] == 0.0 {
				missing++
				n := 0.0
				p := 0.0
				for _, neighbour := range nearby[gp.LSOA] {
					other := gps[neighbour]
					if other != gp && other.ConditionPrevalence[condition] > 0.0 {
						f := float64(1.0 / gp.Location.Distance(other.Location))
						n += f
						p += (f * other.ConditionPrevalence[condition])
					}
				}
				if n > 0.0 {
					imputed++
					gp.ConditionPrevalence[condition] = p / n
				}
			}
		}
	}
	log.Printf("  missing: %d", missing)
	log.Printf("  imputed: %d", imputed)
}

func readGPPractices(w b6.World) (map[GPPracticeCode]*GPPractice, error) {
	f, err := os.Open("data/gp-practices.csv.gz")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	g, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}

	r := csv.NewReader(g)
	r.Comment = '#'
	r.FieldsPerRecord = -1

	gps := make(map[GPPracticeCode]*GPPractice)
	missingLocations := 0
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		var location s2.Point
		var lsoa LSOACode
		postcode := row[GPPracticeDataPostcodeColumn]
		if p := b6.FindPointByID(b6.PointIDFromGBPostcode(postcode), w); p != nil {
			location = p.Point()
			lsoas := w.FindFeatures(b6.Intersection{b6.IntersectsPoint{Point: location}, b6.Tagged{Key: "#boundary", Value: "lsoa"}})
			for lsoas.Next() {
				lsoa = LSOACode(lsoas.Feature().Get("code").Value)
				break
			}
		} else {
			missingLocations++
		}
		code := GPPracticeCode(row[GPPracticeDataCodeColumn])
		gps[code] = &GPPractice{
			Code:     code,
			Name:     row[GPPracticeDataNameColumn],
			ICB:      ICBCode(row[GPPracticeDataICBCodeColumn]),
			Status:   GPPracticeStatus(row[GPPracticeDataStatusColumn]),
			Postcode: postcode,
			Location: location,
			LSOA:     lsoa,
		}
	}
	log.Printf("practices: %d", len(gps))
	log.Printf("  missing locations: %d", missingLocations)
	return gps, nil
}

func buildNearbyGPs(gps map[GPPracticeCode]*GPPractice, radius s1.Angle, w b6.World, cores int) (map[LSOACode][]GPPracticeCode, error) {
	c := make(chan *GPPractice)
	done := make(chan error, 2*cores)
	invalid := s2.Point{}
	seen := make(map[b6.FeatureID]struct{})
	nearby := make(map[LSOACode][]GPPracticeCode)
	practices := 0
	var lock sync.Mutex
	f := func() {
		for gp := range c {
			if gp.Location != invalid {
				cap := s2.CapFromCenterAngle(gp.Location, b6.MetersToAngle(GPLSOANearbyRadiusM))
				lsoas := w.FindFeatures(b6.Intersection{b6.NewIntersectsCap(cap), b6.Tagged{Key: "#boundary", Value: "lsoa"}})
				for lsoas.Next() {
					code := LSOACode(lsoas.Feature().Get("code").Value)
					if code == "" {
						done <- fmt.Errorf("No code for %s", lsoas.FeatureID())
						return
					}
					lock.Lock()
					nearby[code] = append(nearby[code], gp.Code)
					seen[lsoas.FeatureID()] = struct{}{}
					lock.Unlock()
				}
			}
			lock.Lock()
			practices++
			lock.Unlock()
		}
		done <- nil
	}
	for i := 0; i < cores; i++ {
		go f()
	}
	for _, gp := range gps {
		c <- gp
	}
	close(c)
	var err error
	for i := 0; i < cores; i++ {
		if e := <-done; e != nil {
			err = e
		}
	}
	log.Printf("  lsoas served: %d by %d practices", len(seen), practices)
	return nearby, err
}

func readGPPractioners(gps map[GPPracticeCode]*GPPractice) error {
	f, err := os.Open("data/gp-practioners.csv.gz")
	if err != nil {
		return err
	}
	defer f.Close()

	g, err := gzip.NewReader(f)
	if err != nil {
		return err
	}

	r := csv.NewReader(g)
	r.Comment = '#'
	r.FieldsPerRecord = -1
	practioners := 0
	unassigned := 0
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		practioners++
		code := GPPracticeCode(row[GPPractionerDataPracticeCodeColumn])
		if gp, ok := gps[code]; ok {
			gp.Practioners++
		} else {
			unassigned++
		}
	}
	log.Printf("practioners: %d unassigned: %d", practioners, unassigned)
	return nil
}

func readGPAppointments(gps map[GPPracticeCode]*GPPractice) error {
	log.Printf("read GP appointments")
	f, err := os.Open("data/gp-practices-appointments-03-2023.csv.gz")
	if err != nil {
		return err
	}
	defer f.Close()

	g, err := gzip.NewReader(f)
	if err != nil {
		return err
	}

	r := csv.NewReader(g)
	r.Comment = '#'
	columns := make(map[string]int)
	row, err := r.Read()
	if err != nil {
		return err
	}
	for i, column := range row {
		columns[column] = i
	}
	appointments := 0
	matched := 0
	byType := make(map[string]int)
	byCategory := make(map[string]int)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		appointments++
		code := GPPracticeCode(row[columns[GPAppointmentsCodeColumn]])
		t := row[columns[GPAppointmentsHcpTypeColumn]]
		if gp, ok := gps[code]; ok {
			matched++
			if row[columns[GPAppointmentsStatusColumn]] == GPAppointmentsStatusAttended {
				count, err := strconv.Atoi(row[columns[GPAppointmentsCountColumn]])
				if err == nil {
					gp.Appointments += count
					gp.AppointmentsByType[HcpTypeFromString(t)]++
				}
			}
		}
		byType[t]++
		byCategory[row[columns[GPAppointmentsNationalCategory]]]++
	}
	log.Printf("  %d appointments, %d matched", appointments, matched)
	log.Printf("  staff")
	for t, count := range byType {
		log.Printf("    %s: %d", t, count)
	}
	log.Printf("  category")
	for c, count := range byCategory {
		log.Printf("    %s: %d", c, count)
	}
	return nil
}

type Probabilities []float64

func (p Probabilities) Choose() int {
	sample := rand.Float64()
	for i := range p {
		if sample < p[i] {
			return i
		}
		sample -= p[i]
	}
	return len(p) - 1
}

type Sex int

const (
	Male Sex = iota
	Female
	Other

	LastSex   = Other
	Arbitrary = 0
)

func (s Sex) String() string {
	switch s {
	case Male:
		return "m"
	case Female:
		return "f"
	}
	return "o"
}

func sum(xs []int) int {
	s := 0
	for _, x := range xs {
		s += x
	}
	return s
}

func sub(xs []int, ys []int) []int {
	s := make([]int, len(xs))
	for i := range s {
		s[i] = xs[i] - ys[i]
	}
	return s
}

func addf(xs []float64, ys []float64) []float64 {
	s := make([]float64, len(xs))
	for i := range s {
		s[i] = xs[i] + ys[i]
	}
	return s
}

func mulf(xs []float64, ys []float64) []float64 {
	s := make([]float64, len(xs))
	for i := range s {
		s[i] = xs[i] * ys[i]
	}
	return s
}

func ratios(xs []int) []float64 {
	s := sum(xs)
	r := make([]float64, len(xs))
	if s > 0 {
		for i, x := range xs {
			r[i] = float64(x) / float64(s)
		}
	} else {
		for i := range xs {
			r[i] = 1.0 / float64(len(xs))
		}
	}
	return r
}

func normalise(xs []float64) {
	s := 0.0
	for _, x := range xs {
		s += x
	}
	for i := range xs {
		xs[i] /= s
	}
}

func clamp(x float64, min float64, max float64) float64 {
	if x < min {
		return min
	}
	if x > max {
		return max
	}
	return x
}

func makeSexProbabilities(lsoa *LSOA) Probabilities {
	males := sum(lsoa.MalesByAge)
	females := sum(lsoa.FemalesByAge)
	persons := sum(lsoa.PersonsByAge)

	p := make(Probabilities, LastSex+1)
	p[Male] = float64(males) / float64(persons)
	p[Female] = float64(females) / float64(persons)
	p[Other] = float64(persons-males-females) / float64(persons)
	return p
}

func makeAgeProbabilities(lsoa *LSOA) []Probabilities {
	p := make([]Probabilities, LastSex+1)
	p[Male] = Probabilities(ratios(lsoa.MalesByAge))
	p[Female] = Probabilities(ratios(lsoa.FemalesByAge))
	p[Other] = Probabilities(ratios(sub(sub(lsoa.PersonsByAge, lsoa.MalesByAge), lsoa.FemalesByAge)))
	return p
}

type Person struct {
	ID         int
	Sex        Sex
	Age        int
	Home       LSOACode
	GP         GPPracticeCode
	Conditions [QOFConditionLast + 1]bool
}

func PersonHeaderRow() []string {
	return []string{"id", "sex", "age", "home", "gp", "condition_dm", "condition_hyp", "condition_copd"}
}

func presentToString(present bool) string {
	if present {
		return "1"
	}
	return "0"
}

func (p *Person) ToRow() []string {
	return []string{
		strconv.Itoa(p.ID),
		p.Sex.String(),
		strconv.Itoa(p.Age),
		p.Home.String(),
		p.GP.String(),
		presentToString(p.Conditions[QOFConditionDiabetes]),
		presentToString(p.Conditions[QOFConditionHypertension]),
		presentToString(p.Conditions[QOFConditionCOPD]),
	}
}

const (
	// A rough estimate on the maximum size of GP practices lists, used when
	// calculating assignment probabilities of people to practices.
	GPPracticeMaxListSize = 20000
	// GP practices closer than this value to an individual are equally likely
	// to be chosen, after that, it follows the reciprocal, halving at twice
	// the distance.
	GPPracticeEqualDistanceLimitM = 750.0
)

func chooseNearbyGP(lsoa *LSOA, nearbyGPs []GPPracticeCode, gps map[GPPracticeCode]*GPPractice) GPPracticeCode {
	// Remove GPs that don't have any patients (according to the data we have),
	// as many (but not all) seem to be special-case facilities, eg
	// "PARKINSON'S DAY UNIT-CLCH" or "PILOT SE LOCALITY TELEPHONE APPOINTMENTS"
	filtered := make([]GPPracticeCode, 0, len(nearbyGPs))
	for _, gp := range nearbyGPs {
		if gps[gp].ListSize > 0 {
			filtered = append(filtered, gp)
		}
	}
	if len(filtered) == 0 {
		return GPPracticeCodeInvalid
	}
	distances := make([]float64, len(filtered))
	for i, code := range filtered {
		d := b6.AngleToMeters(lsoa.Center.Distance(gps[code].Location))
		if d < GPPracticeEqualDistanceLimitM {
			distances[i] = 1.0
		} else {
			// Half the likelyhood at twice the distance limit away
			distances[i] = 1.0 / (d / GPPracticeEqualDistanceLimitM)
		}
	}
	sizes := make([]float64, len(filtered))
	for i, code := range filtered {
		sizes[i] = clamp(float64(gps[code].ListSize)/GPPracticeMaxListSize, 0.01, 1.0)
	}
	p := mulf(distances, sizes)
	normalise(p)
	return filtered[Probabilities(p).Choose()]
}

func buildPopulation(homes LSOASet, lsoas map[LSOACode]*LSOA, nearbyGPs map[LSOACode][]GPPracticeCode, gps map[GPPracticeCode]*GPPractice) ([]Person, error) {
	people := make([]Person, 0, 1024)
	noPossibleGPs := 0
	for home := range homes {
		if lsoa, ok := lsoas[home]; ok {
			sp := makeSexProbabilities(lsoa)
			ap := makeAgeProbabilities(lsoa)
			possibleGPs := nearbyGPs[home]
			n := sum(lsoa.PersonsByAge)
			for i := 0; i < n; i++ {
				sex := Sex(sp.Choose())
				age := ap[sex].Choose()
				gp := chooseNearbyGP(lsoa, possibleGPs, gps)
				if gp == GPPracticeCodeInvalid {
					noPossibleGPs++
				} else {
					gps[gp].SimulatedListSize++
				}
				people = append(people, Person{ID: len(people), Sex: sex, Age: age, Home: home, GP: gp})
			}
		} else {
			return nil, fmt.Errorf("no LSOA %s", home)
		}
	}
	log.Printf("population:")
	log.Printf("  people: %d", len(people))
	log.Printf("  no possible gps: %d people", noPossibleGPs)
	return people, nil
}

func estimateListSizeError(selected GPPracticeCodeSet, gps map[GPPracticeCode]*GPPractice) float64 {
	n := 0.0
	x := 0.0
	for code := range selected {
		gp := gps[code]
		x += math.Pow(float64(gp.SimulatedListSize-gp.ListSize), 2.0)
		n += 1.0
	}
	return math.Sqrt(x / n)
}

// Return the prevalences for c1|c2 and c1|!c2
func buildJointPrevalence(c1 AgePrevalences, c2 AgePrevalences, c1c2 AgePrevalences, population []Person) (AgePrevalences, AgePrevalences) {
	givenC2 := make(AgePrevalences, len(c1c2))
	givenNotC2 := make(AgePrevalences, len(c1c2))
	for _, sex := range []Sex{Male, Female} {
		for _, a := range c1c2[sex] {
			ec1 := 0.0
			ec2 := 0.0
			n := 0.0
			for _, person := range population {
				if person.Sex == sex && a.AgeRange.Contains(person.Age) {
					n += 1.0
					ec1 += c1.Prevalence(person.Sex, person.Age)
					ec2 += c2.Prevalence(person.Sex, person.Age)
				}
			}
			pc1 := ec1 / n
			pc2 := ec2 / n
			pc1c2 := math.Min(math.Min(a.Prevalence, pc1), pc2)
			p := pc1c2 / pc2
			givenC2[sex] = append(givenC2[sex], AgePrevalence{AgeRange: a.AgeRange, Prevalence: p})
			p = (pc1 - pc1c2) / (1.0 - pc2)
			givenNotC2[sex] = append(givenNotC2[sex], AgePrevalence{AgeRange: a.AgeRange, Prevalence: p})
		}
	}
	return givenC2, givenNotC2
}

func estimateGPPracticeConditionBias(population map[GPPracticeCode][]*Person, condition QOFCondition, prevalence AgePrevalences, gps map[GPPracticeCode]*GPPractice) {
	for code, people := range population {
		gp := gps[code]
		gp.ConditionBias[condition] = 1.0
		if gp.ConditionPrevalence[condition] > 0.0 {
			expected := 0.0
			for _, p := range people {
				expected += prevalence.Prevalence(p.Sex, p.Age)
			}
			if expected > 0.0 {
				gp.ConditionBias[condition] = (float64(len(people)) * gp.ConditionPrevalence[condition]) / float64(expected)
			}
		}
	}
}

func assignConditions(population map[GPPracticeCode][]*Person, conditions []QOFCondition, prevalences Prevalences, joint JointPrevalences, gps map[GPPracticeCode]*GPPractice) {
	shuffled := make([]QOFCondition, len(conditions))
	for i, condition := range conditions {
		shuffled[i] = condition
	}
	swap := func(i int, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	for code, people := range population {
		gp := gps[code]
		for _, p := range people {
			rand.Shuffle(len(shuffled), swap)
			p.Conditions[shuffled[0]] = rand.Float64() < (prevalences[shuffled[0]].Prevalence(p.Sex, p.Age) * gp.ConditionBias[shuffled[0]])
			for i := 1; i < len(shuffled); i++ {
				conditions := JointCondition{Conditions: [2]QOFCondition{shuffled[i], shuffled[i-1]}, SecondPresent: p.Conditions[shuffled[i-1]]}
				if j, ok := joint[conditions]; ok {
					p.Conditions[shuffled[i]] = rand.Float64() < (j.Prevalence(p.Sex, p.Age) * gp.ConditionBias[shuffled[i]])
				} else {
					panic(fmt.Sprintf("no joint probabilities for %+v", conditions))
				}
			}
			for _, condition := range conditions {
				if p.Conditions[condition] {
					gp.SimulatedConditionCounts[condition]++
				}
			}
		}
	}
}

func writeNearbyGPPractices(world b6.World) error {
	log.Printf("build nearby GPs")

	gps, err := readGPPractices(world)
	if err != nil {
		return err
	}

	nearbyGPs, err := buildNearbyGPs(gps, b6.MetersToAngle(GPLSOANearbyRadiusM), world, runtime.NumCPU())
	if err != nil {
		return err
	}

	f, err := os.OpenFile("cached/nearby-gps.csv", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	for lsoa, gps := range nearbyGPs {
		for _, gp := range gps {
			if err := w.Write([]string{lsoa.String(), gp.String()}); err != nil {
				return err
			}
		}
	}
	w.Flush()
	return f.Close()
}

func readNearbyGPPracticess() (map[LSOACode][]GPPracticeCode, error) {
	log.Printf("read: nearby practices")
	f, err := os.Open("cached/nearby-gps.csv")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	nearbyGPs := make(map[LSOACode][]GPPracticeCode)
	r := csv.NewReader(f)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		lsoa := LSOACode(row[0])
		gp := GPPracticeCode(row[1])
		nearbyGPs[lsoa] = append(nearbyGPs[lsoa], gp)
	}
	log.Printf("  %d lsoas", len(nearbyGPs))
	return nearbyGPs, nil
}

func fillCatchmentLSOA(selected GPPracticeCodeSet, gps map[GPPracticeCode]*GPPractice, w b6.World, lsoas LSOASet) {
	r := b6.MetersToAngle(GPLSOANearbyRadiusM)
	for code := range selected {
		cap := s2.CapFromCenterAngle(gps[code].Location, r)
		nearby := w.FindFeatures(b6.Intersection{b6.NewIntersectsCap(cap), b6.Tagged{Key: "#boundary", Value: "lsoa"}})
		for nearby.Next() {
			lsoa := LSOACode(nearby.Feature().Get("code").Value)
			lsoas[lsoa] = struct{}{}
		}
	}
}

type Source struct {
	GPs   map[GPPracticeCode]*GPPractice
	Sites map[ODSCode]*Site
}

func toTagValue(v string) string {
	s := strings.ReplaceAll(strings.ToLower(v), " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(strings.ReplaceAll(s, "(", ""), ")", "")
	return s
}

const NamespaceNHSOrganisation = b6.Namespace("www.datadictionary.nhs.uk/attributes/organisation_code")

func (s *Source) Read(options ingest.ReadOptions, emit ingest.Emit, ctx context.Context) error {
	point := ingest.PointFeature{
		PointID: b6.PointID{
			Namespace: NamespaceNHSOrganisation,
		},
		Tags: []b6.Tag{{Key: "#nhs", Value: "gp_practice"}},
	}
	for code, gp := range s.GPs {
		point.PointID.Value = compact.HashString(string(code))
		point.Location = s2.LatLngFromPoint(gp.Location)
		point.Tags = point.Tags[0:1] // Keep #nhs=gp_practice
		point.Tags = append(point.Tags, b6.Tag{Key: "code", Value: strings.ToLower(string(code))})
		point.Tags = append(point.Tags, b6.Tag{Key: "name", Value: strings.Title(strings.ToLower(gp.Name))})
		point.Tags = append(point.Tags, b6.Tag{Key: "addr:postcode", Value: gp.Postcode})
		if err := emit(&point, 0); err != nil {
			return err
		}
	}

	point.Tags[0].Value = "trust_site"
	for code, site := range s.Sites {
		point.PointID.Value = compact.HashString(string(code))
		point.Location = s2.LatLngFromPoint(site.Location)
		point.Tags = point.Tags[0:1] // Keep #nhs=gp_practice
		point.Tags = append(point.Tags, b6.Tag{Key: "code", Value: strings.ToLower(string(code))})
		point.Tags = append(point.Tags, b6.Tag{Key: "name", Value: strings.Title(strings.ToLower(site.Name))})
		if t := toTagValue(site.Type); t != "" {
			point.Tags = append(point.Tags, b6.Tag{Key: "nhs:site_type", Value: t})
			if strings.Index(t, "hospital") >= 0 {
				point.Tags = append(point.Tags, b6.Tag{Key: "#nhs:hospital", Value: "yes"})

			}
		}
		// TODO: street assignment isn't accurate, as sometimes AddressOne is the name
		// of the facilities, and we should strip the house number and put it in
		// addr:housenumber if not.
		point.Tags = append(point.Tags, b6.Tag{Key: "addr:street", Value: site.Address})
		point.Tags = append(point.Tags, b6.Tag{Key: "addr:postcode", Value: site.Postcode})
		if err := emit(&point, 0); err != nil {
			return err
		}
	}

	boundaries := gdal.Source{
		Filename:   "/vsizip/data/Integrated_Care_Boards_April_2023_EN_BFC_1659257819249669363.zip/ICB_APR_2023_EN_BFC.shp",
		Namespace:  b6.NamespaceUKONSBoundaries,
		IDField:    "ICB23CD",
		IDStrategy: gdal.UKONS2023IDStrategy,
		Bounds:     s2.FullRect(),
		CopyTags:   []gdal.CopyTag{{Key: "name", Field: "ICB23NM"}},
		AddTags:    []b6.Tag{{Key: "#boundary", Value: "nhs_icb"}, {Key: "#nhs", Value: "icb"}},
	}
	return boundaries.Read(options, emit, ctx)
}

type ODSCode string

type Site struct {
	Name     string
	Address  string
	Postcode string
	Location s2.Point
	Type     string
}

func readSites(w b6.World) (map[ODSCode]*Site, error) {
	f, err := os.Open("data/ets.csv")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.Comment = '#'
	missingLocations := 0
	sites := make(map[ODSCode]*Site)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		var location s2.Point
		postcode := row[TrustSitePostcodeColumn]
		if p := b6.FindPointByID(b6.PointIDFromGBPostcode(postcode), w); p != nil {
			location = p.Point()
		} else {
			missingLocations++
		}
		code := ODSCode(row[TrustSiteCodeColumn])
		sites[code] = &Site{
			Name:     row[TrustSiteNameColumn],
			Address:  strings.Title(strings.ToLower(row[TrustSiteAddressOneColumn])),
			Postcode: row[TrustSitePostcodeColumn],
			Location: location,
		}
	}
	log.Printf("sites: %d", len(sites))
	log.Printf("  missing locations: %d", missingLocations)
	return sites, nil
}

func readEstates(sites map[ODSCode]*Site) error {
	f, err := os.Open("data/ERIC - 202122 - Site data.csv")
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.Comment = '#'
	columns := make(map[string]int)
	row, err := r.Read()
	if err != nil {
		return err
	}
	for i, column := range row {
		columns[column] = i
	}

	n := 0
	missingSites := 0
	for {
		n++
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if site, ok := sites[ODSCode(row[columns[EstatesSiteCodeColumn]])]; ok {
			site.Type = row[columns[EstatesSiteTypeColumn]]
		} else {
			missingSites++
		}
	}
	log.Printf("estate returns: %d", n)
	log.Printf("  missing sites: %d", missingSites)
	return nil
}

func writeFeatures(world b6.World) error {
	log.Printf("write features")
	var err error
	var source Source
	source.GPs, err = readGPPractices(world)
	if err != nil {
		return err
	}
	source.Sites, err = readSites(world)
	if err != nil {
		return err
	}
	if err := readEstates(source.Sites); err != nil {
		return err
	}

	config := compact.Options{
		OutputFilename:       "nhs.index",
		Goroutines:           runtime.NumCPU(),
		WorkDirectory:        "",
		PointsWorkOutputType: compact.OutputTypeMemory,
	}
	return compact.Build(&source, &config)
}

type CountJSON struct {
	Value  string
	Counts []int
}

type CountJSONs []CountJSON

func (c CountJSONs) Len() int           { return len(c) }
func (c CountJSONs) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c CountJSONs) Less(i, j int) bool { return c[i].Value < c[j].Value }

type BreakdownJSON struct {
	Key     string
	ByValue CountJSONs
}

type Breakdowns []BreakdownJSON

type PopulationJSON struct {
	TotalListSize                       int
	TotalSimulatedListSize              int
	TotalSimulatedAppointments          int
	Conditions                          []string
	Breakdowns                          Breakdowns
	ByAgeThenCondition                  [][]int
	AppointmentsByAgeThenConditionCount [][]float64
}

// Return an integer representaton of the conditions present for
// the given person, setting bits for condiitons in the order
// passed.
func conditionsToInt(p *Person, conditions []QOFCondition) int {
	i := 0
	for j, condition := range conditions {
		if p.Conditions[condition] {
			i |= (1 << j)
		}
	}
	return i
}

func toJSON(conditions []QOFCondition, people []Person, lsoas map[LSOACode]*LSOA, msoas map[MSOACode]*MSOA, gps map[GPPracticeCode]*GPPractice) *PopulationJSON {
	const maxAge = 100
	n := 1 << len(conditions)
	output := &PopulationJSON{
		Conditions:         make([]string, len(conditions)),
		ByAgeThenCondition: aggregateByAgeThenCondition(people, maxAge, conditions, gps),
	}
	all := BreakdownJSON{Key: "all", ByValue: []CountJSON{{Value: "all", Counts: make([]int, n)}}}
	byMSOA := make(map[MSOACode]*CountJSON)
	byAge := make(CountJSONs, maxAge/10)
	for i := range byAge {
		byAge[i].Value = fmt.Sprintf("%d", i*10)
		byAge[i].Counts = make([]int, n)
	}
	byIMDDecile := make(CountJSONs, 10)
	for i := range byIMDDecile {
		byIMDDecile[i].Value = fmt.Sprintf("%d", i+1)
		byIMDDecile[i].Counts = make([]int, n)
	}
	byIMDDecile[0].Value = "1 (most deprived 10%)"
	byIMDDecile[9].Value = "10 (least deprived 10%)"
	skippedNoMSOA := 0
	icbPeopleByGP := make(map[GPPracticeCode]int)
	for _, p := range people {
		if gps[p.GP].ICB != NorthCentralLondonICBCode {
			continue
		}
		icbPeopleByGP[p.GP]++
		c := conditionsToInt(&p, conditions)
		all.ByValue[0].Counts[c]++
		if msoa, ok := msoas[lsoas[gps[p.GP].LSOA].MSOACode]; ok {
			b, ok := byMSOA[msoa.Code]
			if !ok {
				b = &CountJSON{Value: msoa.Name, Counts: make([]int, n)}
				byMSOA[msoa.Code] = b
			}
			b.Counts[c]++
		} else {
			skippedNoMSOA++
		}
		if a := p.Age / 10; a < len(byAge) {
			byAge[a].Counts[c]++
		} else {
			byAge[len(byAge)-1].Counts[c]++
		}
		byIMDDecile[lsoas[p.Home].IMDDecile-1].Counts[c]++
	}
	log.Printf("skipped: no msoa: %d", skippedNoMSOA)
	for i, condition := range conditions {
		output.Conditions[i] = condition.String()
	}
	output.Breakdowns = append(output.Breakdowns, all)
	msoaBreakdown := BreakdownJSON{
		Key:     "msoa",
		ByValue: make(CountJSONs, 0, len(byMSOA)),
	}
	for _, b := range byMSOA {
		msoaBreakdown.ByValue = append(msoaBreakdown.ByValue, *b)
	}
	sort.Sort(msoaBreakdown.ByValue)
	output.Breakdowns = append(output.Breakdowns, msoaBreakdown)
	output.Breakdowns = append(output.Breakdowns, BreakdownJSON{
		Key:     "age",
		ByValue: byAge,
	})
	output.Breakdowns = append(output.Breakdowns, BreakdownJSON{
		Key:     "imd",
		ByValue: byIMDDecile,
	})

	appointments := make([][]float64, maxAge)
	for age := range appointments {
		appointments[age] = []float64{
			AppointmentsNoConditions.Prevalence(Arbitrary, age),
			AppointmentsOneCondition.Prevalence(Arbitrary, age),
			AppointmentsTwoConditions.Prevalence(Arbitrary, age),
			AppointmentsThreeConditions.Prevalence(Arbitrary, age),
		}
	}
	output.AppointmentsByAgeThenConditionCount = appointments

	for _, gp := range gps {
		if gp.ICB != NorthCentralLondonICBCode {
			continue
		}
		output.TotalListSize += gp.ListSize
		output.TotalSimulatedListSize += gp.SimulatedListSize
		output.TotalSimulatedAppointments += int((float64(gp.Appointments) * float64(gp.SimulatedListSize)) / float64(icbPeopleByGP[gp.Code]))
	}

	return output
}

func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(strings.Replace(strings.TrimSpace(s), ",", "", -1), 64)
}

func averageIMD(people []*Person, lsoas map[LSOACode]*LSOA) float64 {
	total := 0.0
	n := 0
	for _, p := range people {
		total += lsoas[p.Home].IMD
		n++
	}
	if n > 0 {
		return total / float64(n)
	}
	return 0.0
}

func medianAge(people []*Person) int {
	ages := make([]int, len(people))
	for i, p := range people {
		ages[i] = p.Age
	}
	sort.Ints(ages)
	if len(ages) > 0 {
		return ages[len(ages)/2]
	}
	return 0
}

func aggregateByAgeThenCondition(people []Person, maxAge int, conditions []QOFCondition, gps map[GPPracticeCode]*GPPractice) [][]int {
	n := 1 << len(conditions)
	ageThenCondition := make([][]int, maxAge)
	for i := range ageThenCondition {
		ageThenCondition[i] = make([]int, n)
	}
	for _, p := range people {
		if gps[p.GP].ICB != NorthCentralLondonICBCode {
			continue
		}
		c := conditionsToInt(&p, conditions)
		if p.Age < len(ageThenCondition) {
			ageThenCondition[p.Age][c]++
		} else {
			ageThenCondition[len(ageThenCondition)-1][c]++
		}
	}
	return ageThenCondition
}

func writePopulationByAge(aggregated [][]int, conditions []QOFCondition) error {
	f, err := os.OpenFile("output/population-byage.csv", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	headers := []string{"age"}
	for i := 0; i < 1<<len(conditions); i++ {
		parts := []string{}
		for _, c := range conditions {
			if i&(1<<c) != 0 {
				parts = append(parts, c.String())
			}
		}
		if len(parts) > 0 {
			headers = append(headers, strings.Join(parts, "+"))
		} else {
			headers = append(headers, "none")
		}
	}
	headers = append(headers, "total")

	w := csv.NewWriter(f)
	w.Write(headers)
	for age, counts := range aggregated {
		row := []string{strconv.Itoa(age)}
		total := 0
		for _, count := range counts {
			row = append(row, strconv.Itoa(count))
			total += count
		}
		row = append(row, strconv.Itoa(total))
		w.Write(row)
	}
	w.Flush()
	return f.Close()
}

func writePopulation(world b6.World) error {
	log.Printf("read:")
	log.Printf("  icbs")
	icbs, err := readICBs()
	if err != nil {
		return err
	}

	log.Printf("  lsoas")
	lsoas, err := readLSOAs(world)
	if err != nil {
		return err
	}
	msoas, err := fillMSOAs(lsoas)
	if err != nil {
		return err
	}
	if err := fillIMDs(lsoas); err != nil {
		return err
	}

	log.Printf("  gp practices")
	gps, err := readGPPractices(world)
	if err != nil {
		return err
	}

	log.Printf("  lists sizes")
	if err := readGPPracticeListSizes(gps); err != nil {
		return err
	}

	log.Printf("  nearby gp practices")
	nearbyGPs, err := readNearbyGPPracticess()
	if err != nil {
		return err
	}

	log.Printf("  condition prevalence")
	conditions := []QOFCondition{QOFConditionDiabetes, QOFConditionHypertension, QOFConditionCOPD}
	if err := readGPPracticeConditionPrevalence(gps, conditions); err != nil {
		return err
	}

	log.Printf("  condition appointments")
	if err := readGPAppointments(gps); err != nil {
		return err
	}

	log.Printf("  gp practioners")
	if err := readGPPractioners(gps); err != nil {
		return err
	}

	icb := icbs[NorthCentralLondonICBCode]
	icbPopulation := 0
	for code := range icb.LSOAs {
		for _, count := range lsoas[code].PersonsByAge {
			icbPopulation += count
		}
	}
	log.Printf("icb population: %d", icbPopulation)
	icbPractices := make(GPPracticeCodeSet, 0)
	icbPractioners := 0
	for _, gp := range gps {
		if gp.ICB == NorthCentralLondonICBCode {
			icbPractices[gp.Code] = struct{}{}
			icbPractioners += gp.Practioners
		}
	}
	log.Printf("icb practices: %d", len(icbPractices))
	log.Printf("icb practioners: %d", icbPractioners)

	imputeMissingPrevalanceFromNearby(gps, conditions, nearbyGPs)

	homes := make(LSOASet)
	for icb := range icb.LSOAs {
		homes[icb] = struct{}{}
	}
	log.Printf("homes from icb lsoas: %d", len(homes))
	fillCatchmentLSOA(icbPractices, gps, world, homes)
	log.Printf("homes from icb lsoas+buffer: %d", len(homes))

	log.Printf("build population")
	people, err := buildPopulation(homes, lsoas, nearbyGPs, gps)
	if err != nil {
		return err
	}

	log.Printf("list size rmsd: %f", estimateListSizeError(icbPractices, gps))

	joint := make(JointPrevalences)
	for _, condition := range conditions {
		for _, other := range conditions {
			if other != condition {
				given, givenNot := buildJointPrevalence(AllPrevalences[condition], AllPrevalences[other], AllComorbidities[[2]QOFCondition{condition, other}], people)
				log.Printf("p(%s|%s)", condition, other)
				given.Log()
				log.Printf("p(%s|!%s)", condition, other)
				givenNot.Log()
				j := JointCondition{Conditions: [2]QOFCondition{condition, other}, SecondPresent: true}
				joint[j] = given
				j = JointCondition{Conditions: [2]QOFCondition{condition, other}, SecondPresent: false}
				joint[j] = givenNot
			}
		}
	}

	log.Printf("group by gp")
	byPractice := make(map[GPPracticeCode][]*Person)
	for i := range people {
		byPractice[people[i].GP] = append(byPractice[people[i].GP], &people[i])
	}

	log.Printf("estimate bias:")
	for _, condition := range conditions {
		log.Printf("  %s", condition)
		estimateGPPracticeConditionBias(byPractice, condition, AllPrevalences[condition], gps)
	}

	log.Printf("assign conditions")
	assignConditions(byPractice, conditions, AllPrevalences, joint, gps)

	log.Printf("write population")
	f, err := os.OpenFile("output/population.csv", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	w.Write(PersonHeaderRow())
	for _, person := range people {
		if _, ok := icb.LSOAs[person.Home]; ok {
			w.Write(person.ToRow())
		}
	}
	w.Flush()
	f.Close()

	log.Printf("write gps")
	f, err = os.OpenFile("output/gps.csv", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	w = csv.NewWriter(f)
	header := []string{"code", "name", "simulated_list_size", "list_size", "appointments", "appointments_gp", "appointments_other", "population_imd", "median_age"}
	for _, condition := range conditions {
		header = append(header, fmt.Sprintf("prevalence_%s", condition))
	}
	for _, condition := range conditions {
		header = append(header, fmt.Sprintf("bias_%s", condition))
	}
	for _, condition := range conditions {
		header = append(header, fmt.Sprintf("simulated_prevalence_%s", condition))
	}
	w.Write(header)
	totalSimulatedListSize := 0
	for code := range icbPractices {
		gp := gps[code]
		if gp.ICB != NorthCentralLondonICBCode {
			continue
		}
		totalSimulatedListSize += gp.SimulatedListSize
		row := []string{
			code.String(),
			gp.Name,
			strconv.Itoa(gp.SimulatedListSize),
			strconv.Itoa(gp.ListSize),
			strconv.Itoa(gp.Appointments),
			strconv.Itoa(gp.AppointmentsByType[HcpTypeGP]),
			strconv.Itoa(gp.AppointmentsByType[HcpTypeOther]),
			fmt.Sprintf("%f", averageIMD(byPractice[gp.Code], lsoas)),
			strconv.Itoa(medianAge(byPractice[gp.Code])),
		}
		for _, condition := range conditions {
			row = append(row, fmt.Sprintf("%f", gp.ConditionPrevalence[condition]))
		}
		for _, condition := range conditions {
			row = append(row, fmt.Sprintf("%f", gp.ConditionBias[condition]))
		}
		for _, condition := range conditions {
			row = append(row, fmt.Sprintf("%f", float64(gp.SimulatedConditionCounts[condition])/float64(gp.SimulatedListSize)))
		}
		w.Write(row)
	}
	w.Flush()
	if err := f.Close(); err != nil {
		return err
	}
	log.Printf("total simulated list size: %d", totalSimulatedListSize)

	if err := writePopulationByAge(aggregateByAgeThenCondition(people, 100, conditions, gps), conditions); err != nil {
		return err
	}

	output, err := json.Marshal(toJSON(conditions, people, lsoas, msoas, gps))
	if err != nil {
		return err
	}
	f, err = os.OpenFile("output/population.json", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	f.Write(output)
	return f.Close()
}

func main() {
	nearbyGPsFlag := flag.Bool("nearby-gps", false, "Write LSOA to GP mapping")
	populationFlag := flag.Bool("population", false, "Write Population")
	featuresFlag := flag.Bool("features", false, "Write a compact world containing healthcare features")
	worldFlag := flag.String("world", "world/codepoint-open-2023-02.index,world/lsoa-2011.index", "Directory for temporary files, for --memory=false")
	flag.Parse()

	world, err := compact.ReadWorld(*worldFlag, runtime.NumCPU())
	if err != nil {
		log.Fatal(err)
	}

	if *nearbyGPsFlag {
		if err := writeNearbyGPPractices(world); err != nil {
			log.Fatal(err)
		}
	}
	if *featuresFlag {
		if err := writeFeatures(world); err != nil {
			log.Fatal(err)
		}
	}
	if *populationFlag {
		if err := writePopulation(world); err != nil {
			log.Fatal(err)
		}
	}
}
