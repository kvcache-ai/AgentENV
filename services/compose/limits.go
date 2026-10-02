package compose

import (
	"fmt"

	"github.com/compose-spec/compose-go/v2/template"
	"gopkg.in/yaml.v3"
)

// MaxPlanBytes matches the guest's complete, newline-terminated startup frame.
const MaxPlanBytes = 4 * 1024 * 1024

// Allow the host to add empty composeEnv/profiles to a 2 MiB HTTP request.
const MaxRequestBytes = 2*1024*1024 + 64

type expansionBudget struct{ remaining int }

func (b *expansionBudget) take(size int) error {
	if size > b.remaining {
		return fmt.Errorf("Compose expansion exceeds the 4 MiB budget")
	}
	b.remaining -= size
	return nil
}

// Count aliases before decoding them into maps/slices. Charging for each node
// also bounds collections of tiny values, not just large scalar strings.
func validateExpansion(node *yaml.Node) error {
	budget := expansionBudget{MaxPlanBytes}
	active := map[*yaml.Node]bool{}
	var visit func(*yaml.Node, int) error
	visit = func(node *yaml.Node, depth int) error {
		if depth > 128 || active[node] {
			return fmt.Errorf("Compose YAML nesting or recursive alias exceeds the supported limit")
		}
		if err := budget.take(64 + len(node.Value)); err != nil {
			return err
		}
		active[node] = true
		defer delete(active, node)
		if node.Kind == yaml.AliasNode {
			return visit(node.Alias, depth+1)
		}
		for _, child := range node.Content {
			if err := visit(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(node, 0)
}

func (b *expansionBudget) substitute(value string, mapping template.Mapping) (string, error) {
	if err := b.take(len(value)); err != nil {
		return "", err
	}
	var budgetErr error
	result, err := template.Substitute(value, func(key string) (string, bool) {
		if budgetErr != nil {
			return "", true
		}
		resolved, ok := mapping(key)
		// Refuse large replacements before the template package concatenates
		// them, including repeated references within a single scalar.
		budgetErr = b.take(len(resolved))
		if budgetErr != nil {
			return "", true
		}
		return resolved, ok
	})
	if budgetErr != nil {
		return "", budgetErr
	}
	return result, err
}
