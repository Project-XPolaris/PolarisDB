package set

// ExistElement for set hash element
type ExistElement struct{}

type HashSet struct {
	contents map[interface{}]ExistElement
}

func (h *HashSet) RandMember() interface{} {
	if h.Len() == 0 {
		return nil
	}
	for k := range h.contents {
		return k
	}
	return nil
}

func (h *HashSet) Pop() interface{} {
	if h.Len() == 0 {
		return nil
	}
	for k, _ := range h.contents {
		delete(h.contents, k)
		return k
	}
	return nil
}

func (h *HashSet) All() []interface{} {
	result := make([]interface{}, 0)
	for k := range h.contents {
		result = append(result, k)
	}
	return result
}

func (h *HashSet) Len() int {
	return len(h.contents)
}

func (h *HashSet) Add(i interface{}) error {
	h.contents[i] = ExistElement{}
	return nil
}

func (h *HashSet) Remove(i interface{}) error {
	delete(h.contents, i)
	return nil
}

func (h *HashSet) Contains(i interface{}) (bool, error) {
	_, ok := h.contents[i]
	return ok, nil
}

func NewHashSet() *HashSet {
	return &HashSet{
		contents: make(map[interface{}]ExistElement),
	}
}
