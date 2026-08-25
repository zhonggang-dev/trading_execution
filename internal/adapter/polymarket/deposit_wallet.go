package polymarket

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
)

const (
	depositWalletFactory        = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07"
	depositWalletImplementation = "0x58CA52ebe0DadfdF531Cde7062e76746de4Db1eB"
	depositWalletBeacon         = "0x7A18EDfe055488A3128f01F563e5B479D92ffc3a"
)

var (
	erc1967Const1       = mustHex("cc3735a920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3")
	erc1967Const2       = mustHex("5155f3363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076")
	erc1967BeaconConst1 = mustHex("b3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3")
	erc1967BeaconConst2 = mustHex("1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6c")
	erc1967BeaconConst3 = mustHex("60195155f3363d3d373d3d363d602036600436635c60da")
)

func validateDepositWalletAddress(signer, funder string) error {
	uups, beacon, err := deriveDepositWalletAddresses(signer)
	if err != nil {
		return fmt.Errorf("derive POLY_1271 deposit wallet: %w", err)
	}
	if !strings.EqualFold(funder, uups) && !strings.EqualFold(funder, beacon) {
		return fmt.Errorf("POLY_1271 funder is not the signer's deterministic deposit wallet")
	}
	return nil
}

// deriveDepositWalletAddresses returns both deterministic layouts accepted by
// the current migration boundary: legacy UUPS and the post-upgrade beacon
// proxy. Production creation uses the beacon address, while already deployed
// legacy wallets retain their UUPS address.
func deriveDepositWalletAddresses(signer string) (string, string, error) {
	factory, ok := decodeAddress(depositWalletFactory)
	if !ok {
		return "", "", fmt.Errorf("deposit wallet factory is invalid")
	}
	signerBytes, ok := decodeAddress(signer)
	if !ok {
		return "", "", fmt.Errorf("signer address is invalid")
	}
	factoryWord := make([]byte, 32)
	copy(factoryWord[12:], factory)
	walletID := make([]byte, 32)
	copy(walletID[12:], signerBytes)
	args := append(factoryWord, walletID...)
	salt := keccak256(args)

	implementation, ok := decodeAddress(depositWalletImplementation)
	if !ok {
		return "", "", fmt.Errorf("deposit wallet implementation is invalid")
	}
	uupsPrefix, err := clonePrefix("61003D3D8160233D3973", len(args))
	if err != nil {
		return "", "", err
	}
	uupsCode := append(append(append(append([]byte{}, uupsPrefix...), implementation...), 0x60, 0x09), erc1967Const2...)
	uupsCode = append(uupsCode, erc1967Const1...)
	uupsCode = append(uupsCode, args...)

	beaconAddress, ok := decodeAddress(depositWalletBeacon)
	if !ok {
		return "", "", fmt.Errorf("deposit wallet beacon is invalid")
	}
	beaconPrefix, err := clonePrefix("6100523D8160233D3973", len(args))
	if err != nil {
		return "", "", err
	}
	beaconCode := append(append([]byte{}, beaconPrefix...), beaconAddress...)
	beaconCode = append(beaconCode, erc1967BeaconConst3...)
	beaconCode = append(beaconCode, erc1967BeaconConst2...)
	beaconCode = append(beaconCode, erc1967BeaconConst1...)
	beaconCode = append(beaconCode, args...)

	return create2Address(factory, salt, keccak256(uupsCode)),
		create2Address(factory, salt, keccak256(beaconCode)), nil
}

func clonePrefix(baseHex string, argsLength int) ([]byte, error) {
	base, ok := new(big.Int).SetString(baseHex, 16)
	if !ok {
		return nil, fmt.Errorf("invalid clone prefix")
	}
	base.Add(base, new(big.Int).Lsh(big.NewInt(int64(argsLength)), 56))
	value := base.Bytes()
	if len(value) > 10 {
		return nil, fmt.Errorf("clone prefix exceeds ten bytes")
	}
	result := make([]byte, 10)
	copy(result[len(result)-len(value):], value)
	return result, nil
}

func create2Address(factory, salt, initCodeHash []byte) string {
	hash := keccak256([]byte{0xff}, factory, salt, initCodeHash)
	return "0x" + hex.EncodeToString(hash[len(hash)-20:])
}

func mustHex(value string) []byte {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		panic(err)
	}
	return decoded
}
