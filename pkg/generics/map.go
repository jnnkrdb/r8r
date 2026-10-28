package generics

// check whether given map is empty or not, if empty -> create new one
// returns m, or a new empty map if m is nil.
func MapFromNil[M ~map[K]V, K comparable, V any](m M) M {
	if m == nil {
		return make(M)
	}
	return m
}
