package sorter

import (
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

// elements struct with 4 values which will be sorted
type elements struct {
	value1 int
	value2 int
	value3 int
	value4 int
}

type MultiKeySorterTestSuite struct {
	suite.Suite
}

func TestSorterTestSuite(t *testing.T) {
	suite.Run(t, new(MultiKeySorterTestSuite))
}

// TestSorting tests the sorting based on the order specified
func (suite *MultiKeySorterTestSuite) TestSorting() {
	var elementList []interface{}
	elementList = append(elementList, elements{1, 1, 1, 1})
	elementList = append(elementList, elements{1, 1, 1, 4})
	elementList = append(elementList, elements{2, 2, 2, 4})
	elementList = append(elementList, elements{3, 3, 3, 2})
	elementList = append(elementList, elements{3, 3, 3, 2})

	value1 := func(c1, c2 interface{}) bool {
		return c1.(elements).value1 < c2.(elements).value1
	}

	value2 := func(c1, c2 interface{}) bool {
		return c1.(elements).value2 < c2.(elements).value2
	}

	value3 := func(c1, c2 interface{}) bool {
		return c1.(elements).value3 < c2.(elements).value3
	}

	value4 := func(c1, c2 interface{}) bool {
		return c1.(elements).value4 < c2.(elements).value4
	}

	OrderedBy(value4, value1, value2, value3).Sort(elementList)

	suite.EqualValues(elementList[0].(elements),
		elements{1, 1, 1, 1})
	suite.EqualValues(elementList[1].(elements),
		elements{3, 3, 3, 2})
	suite.EqualValues(elementList[2].(elements),
		elements{3, 3, 3, 2})
	suite.EqualValues(elementList[3].(elements),
		elements{1, 1, 1, 4})
	suite.EqualValues(elementList[4].(elements),
		elements{2, 2, 2, 4})

	for _, s := range elementList {
		log.WithFields(log.Fields{
			"values": s.(elements),
		}).Info("Values")
	}

}
