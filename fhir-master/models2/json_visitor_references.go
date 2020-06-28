package models2

type FhirVisitorCollectReferences struct {
	output []string
}

func (v *FhirVisitorCollectReferences) Reference(pos positionInfo, value string) error {
	v.output = append(v.output, value)
	return nil
}
func (v *FhirVisitorCollectReferences) String(pos positionInfo, value string) error {
	return nil
}
func (v *FhirVisitorCollectReferences) Date(pos positionInfo, value string) error {
	return nil
}
func (v *FhirVisitorCollectReferences) Instant(pos positionInfo, value string) error {
	return nil
}
func (v *FhirVisitorCollectReferences) Decimal(pos positionInfo, value string) error {
	return nil
}
func (v *FhirVisitorCollectReferences) Number(pos positionInfo, value string) error {
	return nil
}
func (v *FhirVisitorCollectReferences) Bool(pos positionInfo, value bool) error {
	return nil
}
func (v *FhirVisitorCollectReferences) Null(pos positionInfo) error {
	return nil
}
func (v *FhirVisitorCollectReferences) Extension(pos positionInfo, url string) error {
	return nil
}


func (v *FhirVisitorCollectReferences) GetReferences() []string {
	return v.output
}

func NewFhirVisitorCollectReferences() (*FhirVisitorCollectReferences) {
	return &FhirVisitorCollectReferences{
		output: make([]string, 0, 0),
	}
}


