package middleware

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opencensus.io/trace"
)

type lockId string

type lockRequest struct {
	mutexName   string
	gateChannel chan lockId
}
type unlockRequest struct {
	mutexName string
	lockId    lockId
}

func ClientSpecifiedMutexesMiddleware() gin.HandlerFunc {
	var lockRequests = make(chan *lockRequest)
	var unlockRequests = make(chan *unlockRequest)

	go func() {
		// mutex name ---> map[lockId ---> 'gate channel' on which request awaits]
		mutexes := make(map[string]map[lockId]chan lockId)

		for {
			select {
			case unlockRequest := <-unlockRequests:
				locks, present := mutexes[unlockRequest.mutexName]
				if !present {
					panic("client_specified_mutexes.go: mutex not present during unlock request")
				}
				_, lockIdPresent := locks[unlockRequest.lockId]
				if !lockIdPresent {
					panic("client_specified_mutexes.go: lockId not present during unlock request")
				}

				delete(locks, unlockRequest.lockId)
				if len(locks) > 0 {
					// pick an arbitrary waiter and tell them to proceed
					for lockId, gateChannel := range locks {
						fmt.Printf("[client_specified_mutexes] %s: unlocked & releasing lockId %s\n", unlockRequest.mutexName, lockId)
						gateChannel <- lockId
						break
					}
				} else {
					fmt.Printf("[client_specified_mutexes] %s: unlocked & freed\n", unlockRequest.mutexName)
					delete(mutexes, unlockRequest.mutexName)
				}

			case lockRequest := <-lockRequests:
				newLockId := lockId(uuid.Must(uuid.NewRandom()).String())
				locks, present := mutexes[lockRequest.mutexName]
				if present {
					// add to 'queue'
					locks[newLockId] = lockRequest.gateChannel
					fmt.Printf("[client_specified_mutexes] %s: lock request: queued, newLockId: %s\n", lockRequest.mutexName, newLockId)
				} else {
					// save to a new queue and proceed
					locks := make(map[lockId]chan lockId)
					locks[newLockId] = lockRequest.gateChannel
					lockRequest.gateChannel <- newLockId
					mutexes[lockRequest.mutexName] = locks
					fmt.Printf("[client_specified_mutexes] %s: lock request: proceeding (lockId 0)\n", lockRequest.mutexName)
				}

			}
		}

	}()

	return func(c *gin.Context) {

		mutexName := c.GetHeader("X-Mutex-Name")
		db := c.GetHeader("Db")

		if db != "" {
			// assume re-entrant call (via HandleContext() in routing.go)

		} else if mutexName != "" {

			_, span := trace.StartSpan(c.Request.Context(), "locking mutex")
			span.AddAttributes(trace.StringAttribute("X-Mutex-Name", mutexName))
			lockRequest := &lockRequest{mutexName: mutexName, gateChannel: make(chan lockId)}
			lockRequests <- lockRequest
			lockId := <-lockRequest.gateChannel
			span.End()

			defer func() {
				unlockRequest := &unlockRequest{mutexName, lockId}
				unlockRequests <- unlockRequest
			}()

			c.Header("X-Mutex-Used", "1")
		} else {
			c.Header("X-Mutex-Used", "0")
		}

		c.Next()
	}
}
