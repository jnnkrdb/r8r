package generics

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// interface implementation to enable other ref objects to create identifier hashes as well
type ObjectIdentifier interface {
	GetObjectKind() schema.ObjectKind
	GetNamespace() string
	GetName() string
}

// create the identifier hash from the given object
func CreateIdentifierHashFromObject(o ObjectIdentifier) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s/%s/%s",
		o.GetObjectKind().GroupVersionKind().Group,
		o.GetObjectKind().GroupVersionKind().Version,
		o.GetObjectKind().GroupVersionKind().Kind,
		o.GetNamespace(),
		o.GetName(),
	)))
	return hex.EncodeToString(sum[:8]) // 16 chars
}
