// Copyright (c) 2011-2017, HL7, Inc & The MITRE Corporation
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice, this
//       list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//       this list of conditions and the following disclaimer in the documentation
//       and/or other materials provided with the distribution.
//     * Neither the name of HL7 nor the names of its contributors may be used to
//       endorse or promote products derived from this software without specific
//       prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
// INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
// NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

package models

import (
	"encoding/json"
	"errors"
	"fmt"
)

type HealthcareService struct {
	DomainResource         `bson:",inline"`
	Identifier             []Identifier                              `bson:"identifier,omitempty" json:"identifier,omitempty"`
	Active                 *bool                                     `bson:"active,omitempty" json:"active,omitempty"`
	ProvidedBy             *Reference                                `bson:"providedBy,omitempty" json:"providedBy,omitempty"`
	Category               *CodeableConcept                          `bson:"category,omitempty" json:"category,omitempty"`
	Type                   []CodeableConcept                         `bson:"type,omitempty" json:"type,omitempty"`
	Specialty              []CodeableConcept                         `bson:"specialty,omitempty" json:"specialty,omitempty"`
	Location               []Reference                               `bson:"location,omitempty" json:"location,omitempty"`
	Name                   string                                    `bson:"name,omitempty" json:"name,omitempty"`
	Comment                string                                    `bson:"comment,omitempty" json:"comment,omitempty"`
	ExtraDetails           string                                    `bson:"extraDetails,omitempty" json:"extraDetails,omitempty"`
	Photo                  *Attachment                               `bson:"photo,omitempty" json:"photo,omitempty"`
	Telecom                []ContactPoint                            `bson:"telecom,omitempty" json:"telecom,omitempty"`
	CoverageArea           []Reference                               `bson:"coverageArea,omitempty" json:"coverageArea,omitempty"`
	ServiceProvisionCode   []CodeableConcept                         `bson:"serviceProvisionCode,omitempty" json:"serviceProvisionCode,omitempty"`
	Eligibility            *CodeableConcept                          `bson:"eligibility,omitempty" json:"eligibility,omitempty"`
	EligibilityNote        string                                    `bson:"eligibilityNote,omitempty" json:"eligibilityNote,omitempty"`
	ProgramName            []string                                  `bson:"programName,omitempty" json:"programName,omitempty"`
	Characteristic         []CodeableConcept                         `bson:"characteristic,omitempty" json:"characteristic,omitempty"`
	ReferralMethod         []CodeableConcept                         `bson:"referralMethod,omitempty" json:"referralMethod,omitempty"`
	AppointmentRequired    *bool                                     `bson:"appointmentRequired,omitempty" json:"appointmentRequired,omitempty"`
	AvailableTime          []HealthcareServiceAvailableTimeComponent `bson:"availableTime,omitempty" json:"availableTime,omitempty"`
	NotAvailable           []HealthcareServiceNotAvailableComponent  `bson:"notAvailable,omitempty" json:"notAvailable,omitempty"`
	AvailabilityExceptions string                                    `bson:"availabilityExceptions,omitempty" json:"availabilityExceptions,omitempty"`
	Endpoint               []Reference                               `bson:"endpoint,omitempty" json:"endpoint,omitempty"`
}

// Custom marshaller to add the resourceType property, as required by the specification
func (resource *HealthcareService) MarshalJSON() ([]byte, error) {
	resource.ResourceType = "HealthcareService"
	// Dereferencing the pointer to avoid infinite recursion.
	// Passing in plain old x (a pointer to HealthcareService), would cause this same
	// MarshallJSON function to be called again
	return json.Marshal(*resource)
}

func (x *HealthcareService) GetBSON() (interface{}, error) {
	x.ResourceType = "HealthcareService"
	// See comment in MarshallJSON to see why we dereference
	return *x, nil
}

// The "healthcareService" sub-type is needed to avoid infinite recursion in UnmarshalJSON
type healthcareService HealthcareService

// Custom unmarshaller to properly unmarshal embedded resources (represented as interface{})
func (x *HealthcareService) UnmarshalJSON(data []byte) (err error) {
	x2 := healthcareService{}
	if err = json.Unmarshal(data, &x2); err == nil {
		if x2.Contained != nil {
			for i := range x2.Contained {
				x2.Contained[i], err = MapToResource(x2.Contained[i], true)
				if err != nil {
					return err
				}
			}
		}
		*x = HealthcareService(x2)
		return x.checkResourceType()
	}
	return
}

func (x *HealthcareService) checkResourceType() error {
	if x.ResourceType == "" {
		x.ResourceType = "HealthcareService"
	} else if x.ResourceType != "HealthcareService" {
		return errors.New(fmt.Sprintf("Expected resourceType to be HealthcareService, instead received %s", x.ResourceType))
	}
	return nil
}

type HealthcareServiceAvailableTimeComponent struct {
	BackboneElement    `bson:",inline"`
	DaysOfWeek         []string      `bson:"daysOfWeek,omitempty" json:"daysOfWeek,omitempty"`
	AllDay             *bool         `bson:"allDay,omitempty" json:"allDay,omitempty"`
	AvailableStartTime *FHIRDateTime `bson:"availableStartTime,omitempty" json:"availableStartTime,omitempty"`
	AvailableEndTime   *FHIRDateTime `bson:"availableEndTime,omitempty" json:"availableEndTime,omitempty"`
}

type HealthcareServiceNotAvailableComponent struct {
	BackboneElement `bson:",inline"`
	Description     string  `bson:"description,omitempty" json:"description,omitempty"`
	During          *Period `bson:"during,omitempty" json:"during,omitempty"`
}
