package orderstate

import (
	"github.com/UniPat-AI/trading_execution/internal/domain"
	domainorderstate "github.com/UniPat-AI/trading_execution/internal/domain/orderstate"
)

var (
	ErrIllegalTransition  = domainorderstate.ErrIllegalTransition
	ErrInvalidObservation = domainorderstate.ErrInvalidObservation
)

// Transition 是领域订单状态迁移参数的兼容别名。
type Transition = domainorderstate.Transition

// Apply 将旧服务包调用转发到纯领域订单状态机。
func Apply(current domain.Order, transition Transition) (domain.Order, domain.OrderEvent, error) {
	return domainorderstate.Apply(current, transition)
}
