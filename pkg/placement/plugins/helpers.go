package plugins

import (
	"encoding/json"
)

// TasksByPlacementNeeds is a group of a list of tasks that have the
// same PlacementNeeds.
type TasksByPlacementNeeds struct {
	PlacementNeeds PlacementNeeds
	Tasks          []int
}

// GroupByPlacementNeeds groups the given tasks into a list of
// TasksByPlacementNeedss.
func GroupByPlacementNeeds(tasks []Task) []*TasksByPlacementNeeds {
	groupByPlacementNeeds := map[string]*TasksByPlacementNeeds{}
	for i, task := range tasks {
		key := task.GetPlacementNeeds().ToMapKey()
		if _, found := groupByPlacementNeeds[key]; !found {
			groupByPlacementNeeds[key] = &TasksByPlacementNeeds{
				PlacementNeeds: task.GetPlacementNeeds(),
				Tasks:          []int{},
			}
		}
		groupByPlacementNeeds[key].Tasks = append(groupByPlacementNeeds[key].Tasks, i)
	}
	result := []*TasksByPlacementNeeds{}
	for _, group := range groupByPlacementNeeds {
		result = append(result, group)
	}
	return result
}

// ToMapKey returns a stringified version of the placement needs.
// It zeroes out HostHints first to make sure
func (needs PlacementNeeds) ToMapKey() string {
	needs.HostHints = nil
	content, _ := json.Marshal(needs)
	return string(content)
}
