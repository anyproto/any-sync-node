package account

import (
	"github.com/anytypeio/any-sync-node/config"
	commonaccount "github.com/anytypeio/any-sync/accountservice"
	"github.com/anytypeio/any-sync/app"
	"github.com/anytypeio/any-sync/commonspace/object/accountdata"
	"github.com/anytypeio/any-sync/util/crypto"
)

type service struct {
	accountData *accountdata.AccountKeys
	peerId      string
}

func (s *service) Account() *accountdata.AccountKeys {
	return s.accountData
}

func New() app.Component {
	return &service{}
}

func (s *service) Init(a *app.App) (err error) {
	acc := a.MustComponent(config.CName).(commonaccount.ConfigGetter).GetAccount()

	decodedSigningKey, err := crypto.DecodeKeyFromString(
		acc.SigningKey,
		crypto.UnmarshalEd25519PrivateKey,
		nil)
	if err != nil {
		return err
	}
	decodedPeerKey, err := crypto.DecodeKeyFromString(
		acc.PeerKey,
		crypto.UnmarshalEd25519PrivateKey,
		nil)
	if err != nil {
		return err
	}
	s.accountData = accountdata.New(decodedPeerKey, decodedSigningKey)

	return nil
}

func (s *service) Name() (name string) {
	return commonaccount.CName
}
