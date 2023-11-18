package utils

func SlicesEqual[S ~[]E, E comparable](s1 interface{}, s2 S) bool {
	if s1 == nil && s2 == nil {
		return true
	}
	if s1 == nil || s2 == nil {
		return false
	}

	s1AsSlice, ok := s1.(S)
	if !ok {
		return false
	}

	if len(s1AsSlice) != len(s2) {
		return false
	}

	for i, s1Elem := range s1AsSlice {
		if s1Elem != s2[i] {
			return false
		}
	}
	return true
}

func SlicesEqualIgnoreOrder[S ~[]E, E comparable](s1 interface{}, s2 S) bool {
	if s1 == nil && s2 == nil {
		return true
	}
	if s1 == nil || s2 == nil {
		return false
	}

	s1AsSlice, ok := s1.(S)
	if !ok {
		return false
	}

	if len(s1AsSlice) != len(s2) {
		return false
	}

	s2AsMap := make(map[E]int)
	for _, s2Elem := range s2 {
		s2AsMap[s2Elem]++
	}

	for _, s1Elem := range s1AsSlice {
		if _, ok := s2AsMap[s1Elem]; !ok {
			return false
		}
		s2AsMap[s1Elem]--
		if s2AsMap[s1Elem] == 0 {
			delete(s2AsMap, s1Elem)
		}
	}
	return true
}

func MapsEqual[K comparable, V comparable](m1 interface{}, m2 map[K]V) bool {
	if m1 == nil && m2 == nil {
		return true
	}
	if m1 == nil || m2 == nil {
		return false
	}

	m1AsMap, ok := m1.(map[K]V)
	if !ok {
		return false
	}

	if len(m1AsMap) != len(m2) {
		return false
	}

	for k, v := range m1AsMap {
		if m2[k] != v {
			return false
		}
	}
	return true
}

func MultiMapsEqual[K comparable, V comparable](m1 interface{}, m2 map[K][]V) bool {
	if m1 == nil && m2 == nil {
		return true
	}
	if m1 == nil || m2 == nil {
		return false
	}

	m1AsMap, ok := m1.(map[K][]V)
	if !ok {
		return false
	}

	if len(m1AsMap) != len(m2) {
		return false
	}

	for k, v := range m1AsMap {
		if !SlicesEqualIgnoreOrder(v, m2[k]) {
			return false
		}
	}
	return true
}
