package radix

import (
	"bytes"
	"errors"
	"github.com/projectxpolaris/polarisdb/utils"
)

type RadixTree struct {
	Root *Node
}
type Node struct {
	Value    []byte
	Children []*Node
	Data     []byte
}

func (n *Node) IsLeaf() bool {
	return len(n.Children) == 0
}
func (n *Node) FindChild(key []byte) *Node {
	for _, child := range n.Children {
		if bytes.Equal(child.Value, key) {
			return child
		}
	}
	return nil
}
func NewTree() *RadixTree {
	return &RadixTree{Root: &Node{
		Value:    nil,
		Children: make([]*Node, 0),
	}}
}

func (t *RadixTree) Set(key []byte, data []byte) {
	t.walk(t.Root, key, data)
}

func (t *RadixTree) walk(parent *Node, key []byte, data []byte) {
	if len(key) == 0 {
		parent.Data = data
		return
	}
	var targetNode *Node
	var largestPrefix []byte
	for _, child := range parent.Children {
		// full match
		if utils.IsPrefix(child.Value, key) {
			targetNode = child
			largestPrefix = child.Value
			break
		}
		prefix := utils.FindLargestPrefix(child.Value, key)
		// not self
		if !bytes.Equal(key, prefix) {
			// must longest
			if len(prefix) > len(largestPrefix) {
				targetNode = child
				largestPrefix = prefix
			}
		}
	}
	// not found child
	if targetNode == nil {
		targetNode = &Node{
			Value:    key,
			Children: make([]*Node, 0),
		}
		parent.Children = append(parent.Children, targetNode)
		targetNode.Data = data
		return
	}
	// child node Value is prefix of key
	if utils.IsPrefix(targetNode.Value, key) {
		t.walk(targetNode, key[len(largestPrefix):], data)
		return
	}
	// child node Value is not prefix of key
	// split child node
	newChild := &Node{
		Value:    targetNode.Value[len(largestPrefix):],
		Children: targetNode.Children,
		Data:     targetNode.Data,
	}
	targetNode.Children = []*Node{newChild}
	targetNode.Value = largestPrefix
	targetNode.Data = nil
	key = key[len(largestPrefix):]
	if len(key) > 0 {
		t.walk(targetNode, key, data)
	}
}
func (t *RadixTree) walkGet(parent *Node, key []byte) ([]byte, error) {
	if len(key) == 0 {
		return parent.Data, nil
	}
	var targetNode *Node
	for _, child := range parent.Children {
		if utils.IsPrefix(child.Value, key) {
			targetNode = child
			key = key[len(child.Value):]
			break
		}
	}
	if targetNode == nil {
		return nil, nil
	}

	return t.walkGet(targetNode, key)
}
func (t *RadixTree) Get(key []byte) ([]byte, error) {
	return t.walkGet(t.Root, key)
}

func walkDelete(current *Node, key []byte) error {
	if len(key) == 0 {
		// find child
		current.Data = nil
		return nil
	}

	var targetNode *Node
	for _, child := range current.Children {
		if utils.IsPrefix(child.Value, key) {
			targetNode = child
			key = key[len(child.Value):]
			break
		}
	}
	if current == nil {
		return errors.New("not found")
	}

	err := walkDelete(targetNode, key)
	if err != nil {
		return err
	}
	// remove empty data leaf
	//if targetNode.IsLeaf() && targetNode.Data == nil {
	//	if len(current.Children) == 1 {
	//		current.Children = []*Node{}
	//		return nil
	//	}
	//	for i, child := range current.Children {
	//		if child == targetNode {
	//			current.Children = append(current.Children[:i], current.Children[i+1:]...)
	//			break
	//		}
	//	}
	//}
	return nil
}

func (t *RadixTree) Delete(key []byte) error {
	return walkDelete(t.Root, key)
}

func (t *RadixTree) Walk(hitFunc func(key []byte, value []byte)) {
	t.walkTree(t.Root, []byte{}, hitFunc)
}
func (t *RadixTree) walkTree(parent *Node, key []byte, hitFunc func(key []byte, value []byte)) {
	if len(parent.Value) > 0 {
		key = append(key, parent.Value...)
	}
	if parent.Data != nil {
		hitFunc(key, parent.Data)
	}
	if !parent.IsLeaf() {
		for _, child := range parent.Children {
			t.walkTree(child, key, hitFunc)
		}
	}
}
