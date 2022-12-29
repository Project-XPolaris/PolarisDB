/*
	https://www.epaperpress.com/sortsearch/download/skiplist.pdf
*/

package skiplist

import (
	"math"
	"math/rand"
	"sort"
)

const (
	SKIPLIST_MAXLEVEL    = 32   /* For 2^32 elements */
	SKIPLIST_Probability = 0.25 /* Skiplist probability = 1/4 */
)

type (
	/*
		Links:
		- http://blog.wjin.org/posts/redis-internal-data-structure-skiplist.html
		- https://developpaper.com/skip-list-lookup-tree-btree-in-redis-mysql/


		-   Redis’s skip table implementation is composed of two structures: zskiplist and zskiplistnode. Zskiplist is
			used to save the information of jump table (such as header node, tail node and length), while zskiplistnode
			is used to represent hop table node
		-   The layer height of each hop table node is a random number between 1 and 32 (the program is generated according
			to the power law, the greater the number, the smaller the probability of occurrence)
		-   In the same hop table, multiple nodes can contain the same score, but the member object of each node must be unique
		-   The nodes in the jump table are sorted according to the score. When the score is the same, the nodes are sorted
			according to the size of the member object
		-   Each node of the original linked list layer has a pointer to the previous node, which is used to iterate from the
			tail direction to the header direction (used when executing the command of reverse order processing ordered set
			such as zrevrange or zrevrangebyscale)




				https://github.com/redis/redis/blob/unstable/src/server.h

				// ZSETs use a specialized version of Skiplists

				typedef struct zskiplistNode {
				    sds ele;
				    double score;
				    struct zskiplistNode *backward;
				    struct zskiplistLevel {
				        struct zskiplistNode *forward;
				        unsigned long span;
				    } level[];
				} zskiplistNode;

				typedef struct zskiplist {
				    struct zskiplistNode *header, *tail;
				    unsigned long length;
				    int level;
				} zskiplist;

				typedef struct Zset {
				    dict *dict;
				    zskiplist *zsl;
				} Zset;

	*/

	zskiplistLevel struct {
		forward *zskiplistNode
		span    uint64
	}

	// (hop table)
	zskiplistNode struct {
		member   string
		value    interface{}
		score    float64
		backward *zskiplistNode
		level    []*zskiplistLevel
	}

	// Node in skip list (jump table)
	zskiplist struct {
		head   *zskiplistNode
		tail   *zskiplistNode
		length int64
		level  int
	}

	Zset struct {
		dict map[string]*zskiplistNode
		zsl  *zskiplist
	}
)

// Returns a random level for the new skiplist node we are going to create.
// The return value of this function is between 1 and SKIPLIST_MAXLEVEL
// (both inclusive), with a powerlaw-alike distribution where higher
// levels are less likely to be returned.
func randomLevel() int {
	level := 1
	for float64(rand.Int31()&0xFFFF) < float64(SKIPLIST_Probability*0xFFFF) {
		level += 1
	}
	if level < SKIPLIST_MAXLEVEL {
		return level
	}

	return SKIPLIST_MAXLEVEL
}

func createNode(level int, score float64, member string, value interface{}) *zskiplistNode {
	node := &zskiplistNode{
		score:  score,
		member: member,
		value:  value,
		level:  make([]*zskiplistLevel, level),
	}

	for i := range node.level {
		node.level[i] = new(zskiplistLevel)
	}

	return node
}

func newZSkipList() *zskiplist {
	return &zskiplist{
		level: 1,
		head:  createNode(SKIPLIST_MAXLEVEL, 0, "", nil),
	}
}

/*
	Insert a new node in the skiplist. Assumes the element does not already
	exist (up to the caller to enforce that). The skiplist takes ownership
	of the passed member string.
*/
func (z *zskiplist) insert(score float64, member string, value interface{}) *zskiplistNode {
	/*

		https://www.youtube.com/watch?v=UGaOXaXAM5M
		https://www.youtube.com/watch?v=NDGpsfwAaqo

		The update array stores previous pointers for each level, new node
		will be added after them. rank array stores the rank value of each skiplist node.

		Steps:

		generate update and rank array
		create a new node with random level
		insert new node according to update and rank info
		update other necessary infos, such as span, backward pointer, length.
	*/

	updates := make([]*zskiplistNode, SKIPLIST_MAXLEVEL)
	rank := make([]uint64, SKIPLIST_MAXLEVEL)

	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		/* store rank that is crossed to reach the insert position */
		if i == z.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		if x.level[i] != nil {
			for x.level[i].forward != nil &&
				(x.level[i].forward.score < score ||
					(x.level[i].forward.score == score && // score is the same but the key is different
						x.level[i].forward.member < member)) {

				rank[i] += x.level[i].span
				x = x.level[i].forward
			}
		}
		updates[i] = x
	}

	/* we assume the key is not already inside, since we allow duplicated
	 * scores, and the re-insertion of score and redis object should never
	 * happen since the caller of Insert() should test in the hash table
	 * if the element is already inside or not. */
	level := randomLevel()
	if level > z.level { // add a new level
		for i := z.level; i < level; i++ {
			rank[i] = 0
			updates[i] = z.head
			updates[i].level[i].span = uint64(z.length)
		}
		z.level = level
	}

	x = createNode(level, score, member, value)
	for i := 0; i < level; i++ {
		x.level[i].forward = updates[i].level[i].forward
		updates[i].level[i].forward = x

		/* update span covered by update[i] as x is inserted here */
		x.level[i].span = updates[i].level[i].span - (rank[0] - rank[i])
		updates[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	/* increment span for untouched levels */
	for i := level; i < z.level; i++ {
		updates[i].level[i].span++
	}

	if updates[0] == z.head {
		x.backward = nil
	} else {
		x.backward = updates[0]
	}

	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		z.tail = x
	}

	z.length++
	return x
}

/* Internal function used by delete, DeleteByScore and DeleteByRank */
func (z *zskiplist) deleteNode(x *zskiplistNode, updates []*zskiplistNode) {
	for i := 0; i < z.level; i++ {
		if updates[i].level[i].forward == x {
			updates[i].level[i].span += x.level[i].span - 1
			updates[i].level[i].forward = x.level[i].forward
		} else {
			updates[i].level[i].span--
		}
	}

	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		z.tail = x.backward
	}

	for z.level > 1 && z.head.level[z.level-1].forward == nil {
		z.level--
	}

	z.length--
}

/* Delete an element with matching score/key from the skiplist. */
func (z *zskiplist) delete(score float64, member string) {
	update := make([]*zskiplistNode, SKIPLIST_MAXLEVEL)

	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member < member)) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	/* We may have multiple elements with the same score, what we need
	 * is to find the element with both the right score and object. */
	x = x.level[0].forward
	if x != nil && score == x.score && x.member == member {
		z.deleteNode(x, update)
		return
	}
}

// Find the rank of the node specified by key
// Note that the rank is 0-based integer. Rank 0 means the first node
func (z *zskiplist) getRank(score float64, member string) int64 {
	var rank uint64 = 0
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score &&
					x.level[i].forward.member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		if x.member == member {
			return int64(rank)
		}
	}

	return 0
}

func (z *zskiplist) getNodeByRank(rank uint64) *zskiplistNode {
	var traversed uint64 = 0

	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(traversed+x.level[i].span) <= rank {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		if traversed == rank {
			return x
		}
	}

	return nil
}

/*
	Zset node utility
*/

func (z *Zset) getNodeByRank(rank int64, reverse bool) (string, float64) {
	if rank < 0 || rank > z.zsl.length {
		return "", math.MinInt64
	}

	if reverse {
		rank = z.zsl.length - rank
	} else {
		rank++
	}

	n := z.zsl.getNodeByRank(uint64(rank))
	if n == nil {
		return "", math.MinInt64
	}

	node := z.dict[n.member]
	if node == nil {
		return "", math.MinInt64
	}

	return node.member, node.score

}

func (z *Zset) findRange(start, stop int64, reverse bool, withScores bool) (val []interface{}) {
	length := z.zsl.length

	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}

	if stop < 0 {
		stop += length
	}

	if start > stop || start >= length {
		return
	}

	if stop >= length {
		stop = length - 1
	}
	span := (stop - start) + 1

	var node *zskiplistNode
	if reverse {
		node = z.zsl.tail
		if start > 0 {
			node = z.zsl.getNodeByRank(uint64(length - start))
		}
	} else {
		node = z.zsl.head.level[0].forward
		if start > 0 {
			node = z.zsl.getNodeByRank(uint64(start + 1))
		}
	}

	for span > 0 {
		span--
		if withScores {
			val = append(val, node.member, node.score)
		} else {
			val = append(val, node.member)
		}
		if reverse {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}

	return
}

func NewZset() *Zset {
	return &Zset{
		dict: make(map[string]*zskiplistNode),
		zsl:  newZSkipList(),
	}
}

func (z *Zset) IsExists(key string) bool {
	return z.dict[key] != nil
}
func (z *Zset) Add(score float64, member string, value interface{}) (val int) {
	v, exist := z.dict[member]
	var node *zskiplistNode
	if exist {
		val = 0
		// score changes, delete and re-insert
		if score != v.score {
			z.zsl.delete(v.score, member)
			node = z.zsl.insert(score, member, value)
		} else {
			// score does not change, update value
			v.value = value
		}
	} else {
		val = 1
		node = z.zsl.insert(score, member, value)
	}
	if node != nil {
		z.dict[member] = node
	}
	return
}

// ZScore returns the score of member in the sorted set at key.
func (z *Zset) ZScore(member string) (ok bool, score float64) {
	node, exist := z.dict[member]
	if !exist {
		return
	}
	return true, node.score
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (z *Zset) ZCard() int {
	return len(z.dict)
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
func (z *Zset) ZRank(member string) int64 {
	v, exist := z.dict[member]
	if !exist {
		return -1
	}

	rank := z.zsl.getRank(v.score, member)
	rank--

	return rank
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, which means that the member with the highest score has rank 0.
func (z *Zset) ZRevRank(member string) int64 {

	v, exist := z.dict[member]
	if !exist {
		return -1
	}

	rank := z.zsl.getRank(v.score, member)

	return z.zsl.length - rank
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
// If key does not exist, a new sorted set with the specified member as its sole member is created.
func (z *Zset) ZIncrBy(increment float64, member string) float64 {
	var memberExists bool
	node, memberExists := z.dict[member]
	if memberExists {
		increment += node.score
		z.Add(increment, member, node.value)
	}
	return increment
}

// ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
// An error is returned when key exists and does not hold a sorted set.
func (z *Zset) ZRem(member string) bool {
	v, exist := z.dict[member]
	if exist {
		z.zsl.delete(v.score, member)
		delete(z.dict, member)
		return true
	}

	return false
}

// ZScoreRange returns all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
// The elements are considered to be ordered from low to high scores.
func (z *Zset) ZScoreRange(min, max float64) (val []interface{}) {
	if min > max {
		return
	}

	item := z.zsl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}

	x := item.head
	for i := item.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.score < min {
			x = x.level[i].forward
		}
	}

	x = x.level[0].forward
	for x != nil {
		if x.score > max {
			break
		}

		val = append(val, x.member, x.score)
		x = x.level[0].forward
	}

	return
}

// ZRevScoreRange returns all the elements in the sorted set at key with a score between max and min (including elements with score equal to max or min).
// In contrary to the default ordering of sorted sets, for this command the elements are considered to be ordered from high to low scores.
func (z *Zset) ZRevScoreRange(max, min float64) (val []interface{}) {
	if max < min {
		return
	}

	item := z.zsl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}

	x := item.head
	for i := item.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.score <= max {
			x = x.level[i].forward
		}
	}

	for x != nil {
		if x.score < min {
			break
		}

		val = append(val, x.member, x.score)
		x = x.backward
	}

	return
}

// ZRange returns the specified range of elements in the sorted set stored at <key>.
func (z *Zset) ZRange(start, stop int) []interface{} {
	return z.findRange(int64(start), int64(stop), false, false)
}

// ZRangeWithScores returns the specified range of elements in the sorted set stored at <key>.
func (z *Zset) ZRangeWithScores(start, stop int) []interface{} {
	return z.findRange(int64(start), int64(stop), false, true)
}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (z *Zset) ZRevRange(start, stop int) []interface{} {
	return z.findRange(int64(start), int64(stop), true, false)
}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (z *Zset) ZRevRangeWithScores(start, stop int) []interface{} {
	return z.findRange(int64(start), int64(stop), true, true)
}

// ZGetByRank gets the member at key by rank, the rank is ordered from lowest to highest.
// The rank of lowest is 0 and so on.
func (z *Zset) ZGetByRank(rank int) (val []interface{}) {
	member, score := z.getNodeByRank(int64(rank), false)
	val = append(val, member, score)
	return
}

// ZRevGetByRank get the member at key by rank, the rank is ordered from highest to lowest.
// The rank of highest is 0 and so on.
func (z *Zset) ZRevGetByRank(rank int) (val []interface{}) {
	member, score := z.getNodeByRank(int64(rank), true)
	val = append(val, member, score)
	return
}

// get and remove the element with minimal score, nil if the set is empty
func (z *Zset) ZPopMin() (rec *zskiplistNode) {
	x := z.zsl.head.level[0].forward
	if x != nil {
		z.ZRem(x.member)
	}
	return x
}

// get and remove the element with maximum score, nil if the set is empty
func (z *Zset) ZPopMax(key string) (rec *zskiplistNode) {
	x := z.zsl.tail
	if x != nil {
		z.ZRem(x.member)
	}
	return x
}

type ZRangeOptions struct {
	Limit        int  // limit the max nodes to return
	ExcludeStart bool // exclude start value, so it search in interval (start, end] or (start, end)
	ExcludeEnd   bool // exclude end value, so it search in interval [start, end) or (start, end)
}

/*
	Returns all the elements in the sorted set at key with a score between min and max (including
	elements with score equal to min or max). The elements are considered to be ordered from low to
	high scores.

	If options is nil, it searchs in interval [start, end] without any limit by default

	https://github.com/wangjia184/sortedset/blob/af6d6d227aa79e2a64b899d995ce18aa0bef437c/sortedset.go#L283
*/
func (z *Zset) ZRangeByScore(start float64, end float64, options *ZRangeOptions) (nodes []*zskiplistNode) {

	zsl := z.zsl

	// prepare parameters
	var limit int = int((^uint(0)) >> 1)
	if options != nil && options.Limit > 0 {
		limit = options.Limit
	}

	excludeStart := options != nil && options.ExcludeStart
	excludeEnd := options != nil && options.ExcludeEnd
	reverse := start > end
	if reverse {
		start, end = end, start
		excludeStart, excludeEnd = excludeEnd, excludeStart
	}

	//determine if out of range
	if zsl.length == 0 {
		return nodes
	}

	if reverse { // search from end to start
		x := zsl.head

		if excludeEnd {
			for i := zsl.level - 1; i >= 0; i-- {
				for x.level[i].forward != nil &&
					x.level[i].forward.score < end {
					x = x.level[i].forward
				}
			}
		} else {
			for i := zsl.level - 1; i >= 0; i-- {
				for x.level[i].forward != nil &&
					x.level[i].forward.score <= end {
					x = x.level[i].forward
				}
			}
		}

		for x != nil && limit > 0 {
			if excludeStart {
				if x.score <= start {
					break
				}
			} else {
				if x.score < start {
					break
				}
			}

			next := x.backward

			nodes = append(nodes, x)
			limit--

			x = next
		}
	} else {
		// search from start to end
		x := zsl.head
		if excludeStart {
			for i := zsl.level - 1; i >= 0; i-- {
				for x.level[i].forward != nil &&
					x.level[i].forward.score <= start {
					x = x.level[i].forward
				}
			}
		} else {
			for i := zsl.level - 1; i >= 0; i-- {
				for x.level[i].forward != nil &&
					x.level[i].forward.score < start {
					x = x.level[i].forward
				}
			}
		}

		/* Current node is the last with score < or <= start. */
		x = x.level[0].forward

		for x != nil && limit > 0 {
			if excludeEnd {
				if x.score >= end {
					break
				}
			} else {
				if x.score > end {
					break
				}
			}

			next := x.level[0].forward

			nodes = append(nodes, x)
			limit--

			x = next
		}
	}

	return nodes
}

func ZsetDiff(targetSet *Zset, otherSets ...*Zset) *Zset {
	selectMethod := 1
	totalOtherCount := 0
	for _, set := range otherSets {
		totalOtherCount += set.ZCard()
	}

	if targetSet.ZCard()*len(otherSets) > totalOtherCount+targetSet.ZCard() {
		selectMethod = 2
	}
	resultSet := NewZset()
	if selectMethod == 1 {
		sort.Slice(otherSets, func(i, j int) bool {
			return otherSets[i].ZCard() < otherSets[j].ZCard()
		})
		//O(N*M)
		for targetMember, targetNode := range targetSet.dict {
			existFlag := true
			for _, otherSet := range otherSets {
				if _, ok := otherSet.dict[targetMember]; !ok {
					existFlag = false
					break
				}
			}
			if !existFlag {
				resultSet.Add(targetNode.score, targetMember, nil)
			}
		}
	} else {
		targetSetKeys := make([]string, 0)
		for key, _ := range targetSet.dict {
			targetSetKeys = append(targetSetKeys, key)
		}
		for _, otherSet := range otherSets {
			for otherMember := range otherSet.dict {
				for i, targetMember := range targetSetKeys {
					if targetMember == otherMember {
						targetSetKeys = append(targetSetKeys[:i], targetSetKeys[i+1:]...)
						break
					}
				}
			}
		}
		for _, targetMember := range targetSetKeys {
			resultSet.Add(targetSet.dict[targetMember].score, targetMember, nil)
		}
	}
	return resultSet
}

func ZsetInter(sets ...*Zset) *Zset {
	resultZset := NewZset()
	sort.Slice(sets, func(i, j int) bool {
		return sets[i].ZCard() < sets[j].ZCard()
	})
	for targetMember, targetNode := range sets[0].dict {
		existFlag := true
		scoreAns := targetNode.score
		for _, otherSet := range sets[1:] {
			if _, ok := otherSet.dict[targetMember]; !ok {
				existFlag = false
				break
			} else {
				scoreAns += otherSet.dict[targetMember].score
			}
		}
		if existFlag {
			resultZset.Add(scoreAns, targetMember, nil)
		}
	}
	return resultZset
}

func ZsetUnion(sets ...*Zset) *Zset {
	resultZset := NewZset()
	for _, set := range sets {
		for key, node := range set.dict {
			if resultZset.IsExists(key) {
				resultZset.ZIncrBy(node.score, key)
			} else {
				resultZset.Add(node.score, key, nil)
			}
		}
	}
	return resultZset
}
