package main

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	cbg "github.com/whyrusleeping/cbor-gen"
	"math"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	parmap "github.com/filecoin-project/lotus/lib/parmap"
)

func runSyncer(ctx context.Context, api api.FullNode, st *storage, maxBatch int) {
	notifs, err := api.ChainNotify(ctx)
	if err != nil {
		panic(err)
	}
	go func() {
		for notif := range notifs {
			for _, change := range notif {
				switch change.Type {
				case store.HCCurrent:
					fallthrough
				case store.HCApply:
					syncHead(ctx, api, st, change.Val, maxBatch)
				case store.HCRevert:
					log.Warnf("revert todo")
				}

				if change.Type == store.HCCurrent {
					go subMpool(ctx, api, st)
					go subBlocks(ctx, api, st)
				}
			}
		}
	}()
}

type minerKey struct {
	addr      address.Address
	act       types.Actor
	stateroot cid.Cid
	tsKey     types.TipSetKey
}

type minerInfo struct {
	state miner.State
	info  miner.MinerInfo

	rawPower big.Int
	qalPower big.Int
	ssize    uint64
	psize    uint64
}

type newMinerInfo struct {
	// common
	addr      address.Address
	act       types.Actor
	stateroot cid.Cid

	// miner specific
	state miner.State
	info  miner.MinerInfo

	// tracked by power actor
	rawPower big.Int
	qalPower big.Int
	ssize    uint64
	psize    uint64
}

type actorInfo struct {
	stateroot cid.Cid
	tsKey     types.TipSetKey
	state     string
}

func syncHead(ctx context.Context, api api.FullNode, st *storage, headTs *types.TipSet, maxBatch int) {
	var alk sync.Mutex

	log.Infof("Getting synced block list")

	hazlist := st.hasList()

	log.Infof("Getting headers / actors")

	allToSync := map[cid.Cid]*types.BlockHeader{}
	toVisit := list.New()

	for _, header := range headTs.Blocks() {
		toVisit.PushBack(header)
	}

	for toVisit.Len() > 0 {
		bh := toVisit.Remove(toVisit.Back()).(*types.BlockHeader)

		_, has := hazlist[bh.Cid()]
		if _, seen := allToSync[bh.Cid()]; seen || has {
			continue
		}

		allToSync[bh.Cid()] = bh

		if len(allToSync)%500 == 10 {
			log.Infof("todo: (%d) %s @%d", len(allToSync), bh.Cid(), bh.Height)
		}

		if len(bh.Parents) == 0 {
			continue
		}

		pts, err := api.ChainGetTipSet(ctx, types.NewTipSetKey(bh.Parents...))
		if err != nil {
			log.Error(err)
			continue
		}

		for _, header := range pts.Blocks() {
			toVisit.PushBack(header)
		}
	}

	for len(allToSync) > 0 {
		actors := map[address.Address]map[types.Actor]actorInfo{}
		addressToID := map[address.Address]address.Address{}
		minH := abi.ChainEpoch(math.MaxInt64)

		for _, header := range allToSync {
			if header.Height < minH {
				minH = header.Height
			}
		}

		toSync := map[cid.Cid]*types.BlockHeader{}
		for c, header := range allToSync {
			if header.Height < minH+abi.ChainEpoch(maxBatch) {
				toSync[c] = header
				addressToID[header.Miner] = address.Undef
			}
		}
		for c := range toSync {
			delete(allToSync, c)
		}

		log.Infow("Starting Sync", "numBlocks", len(toSync), "maxBatch", maxBatch)
		tipToState := make(map[types.TipSetKey]cid.Cid)

		paDone := 0
		parmap.Par(50, parmap.MapArr(toSync), func(bh *types.BlockHeader) {
			paDone++
			if paDone%100 == 0 {
				log.Infof("pa: %d %d%%", paDone, (paDone*100)/len(toSync))
			}

			if len(bh.Parents) == 0 { // genesis case
				genesisTs, _ := types.NewTipSet([]*types.BlockHeader{bh})
				aadrs, err := api.StateListActors(ctx, genesisTs.Key())
				if err != nil {
					log.Error(err)
					return
				}

				// TODO suspicious there is not a lot to be gained by doing this in parallel since the genesis state
				// is unlikely to contain a lot of actors, why not for loop here?
				parmap.Par(50, aadrs, func(addr address.Address) {
					act, err := api.StateGetActor(ctx, addr, genesisTs.Key())
					if err != nil {
						log.Error(err)
						return
					}
					ast, err := api.StateReadState(ctx, act, genesisTs.Key())
					if err != nil {
						log.Error(err)
						return
					}
					state, err := json.Marshal(ast.State)
					if err != nil {
						log.Error(err)
						return
					}

					alk.Lock()
					_, ok := actors[addr]
					if !ok {
						actors[addr] = map[types.Actor]actorInfo{}
					}
					actors[addr][*act] = actorInfo{
						stateroot: bh.ParentStateRoot,
						tsKey:     genesisTs.Key(),
						state:     string(state),
					}
					addressToID[addr] = address.Undef
					tipToState[genesisTs.Key()] = bh.ParentStateRoot
					alk.Unlock()
				})

				return
			}

			pts, err := api.ChainGetTipSet(ctx, types.NewTipSetKey(bh.Parents...))
			if err != nil {
				log.Error(err)
				return
			}

			// TODO I'm pretty sure this is returning deleted actors as well..
			changes, err := api.StateChangedActors(ctx, pts.ParentState(), bh.ParentStateRoot)
			if err != nil {
				log.Error(err)
				return
			}

			for a, act := range changes {
				act := act

				addr, err := address.NewFromString(a)
				if err != nil {
					log.Error(err)
					return
				}

				ast, err := api.StateReadState(ctx, &act, pts.Key())
				if err != nil {
					log.Error(err)
					return
				}

				state, err := json.Marshal(ast.State)
				if err != nil {
					log.Error(err)
					return
				}

				alk.Lock()
				_, ok := actors[addr]
				if !ok {
					actors[addr] = map[types.Actor]actorInfo{}
				}
				// a change occurred for the actor with address `addr` and state `act` at tipset `pts`.
				actors[addr][act] = actorInfo{
					stateroot: bh.ParentStateRoot,
					state:     string(state),
					tsKey:     pts.Key(),
				}
				addressToID[addr] = address.Undef
				// processing tipset `pts` produced stateroot `bh.ParentStateRoot`
				tipToState[pts.Key()] = bh.ParentStateRoot
				alk.Unlock()
			}
		})

		log.Infow("Sync Complete", "numTipSets", len(tipToState))

		log.Infof("Getting messages")

		msgs, incls := fetchMessages(ctx, api, toSync)

		log.Infof("Resolving addresses")

		for _, message := range msgs {
			addressToID[message.To] = address.Undef
			addressToID[message.From] = address.Undef
		}

		parmap.Par(50, parmap.KMapArr(addressToID), func(addr address.Address) {
			// FIXME: cannot use EmptyTSK here since actorID's can change during reorgs, need to use the corresponding tipset.
			// TODO: figure out a way to get the corresponding tipset...
			raddr, err := api.StateLookupID(ctx, addr, types.EmptyTSK)
			if err != nil {
				log.Warn(err)
				return
			}
			alk.Lock()
			addressToID[addr] = raddr
			alk.Unlock()
		})

		log.Infof("Getting miner info")

		miners := map[minerKey]*minerInfo{}

		minerTips := make(map[types.TipSetKey][]*newMinerInfo)

		headsSeen := make(map[cid.Cid]bool)

		minerChanges := 0
		for addr, m := range actors {
			for actor, c := range m {
				if actor.Code != builtin.StorageMinerActorCodeID {
					continue
				}

				// only want miner actors with head change events
				if headsSeen[actor.Head] {
					continue
				}
				minerChanges++

				minerTips[c.tsKey] = append(minerTips[c.tsKey], &newMinerInfo{
					addr:      addr,
					act:       actor,
					stateroot: c.stateroot,

					state: miner.State{},
					info:  miner.MinerInfo{},

					rawPower: big.Zero(),
					qalPower: big.Zero(),
					ssize:    0,
					psize:    0,
				})

				miners[minerKey{
					addr:      addr,
					act:       actor,
					stateroot: c.stateroot,
					tsKey:     c.tsKey,
				}] = &minerInfo{}
				headsSeen[actor.Head] = true
			}
		}

		minerProcessingState := time.Now()
		log.Infow("Processing miners", "numTips", len(minerTips), "numMinerChanges", minerChanges)
		parmap.Par(50, parmap.KVMapArr(minerTips), func(it func() (types.TipSetKey, []*newMinerInfo)) {
			tsKey, minerInfo := it()

			//
			// Get miner raw and quality power
			//
			mp, err := getPowerActorClaimsMap(ctx, api, tsKey)
			if err != nil {
				log.Error(err)
				return
			}
			log.Debugw("populating miner info", "tipset", tsKey.String(), "numMiners", len(minerInfo))
			for _, mi := range minerInfo {
				err := mp.ForEach(nil, func(key string) error {
					addr, err := address.NewFromBytes([]byte(key))
					if err != nil {
						return err
					}
					var claim power.Claim
					keyerAddr := adt.AddrKey(addr)
					found, err := mp.Get(keyerAddr, &claim)
					if err != nil {
						return err
					}
					// means the miner didn't have a claim at this stateroot
					if !found {
						return nil
					}

					if claim.QualityAdjPower.Int64() == 0 {
						return nil
					}

					mi.rawPower = claim.RawBytePower
					mi.qalPower = claim.QualityAdjPower
					return nil
				})
				if err != nil {
					log.Error(err)
				}

				//
				// Get the miner state info
				//
				astb, err := api.ChainReadObj(ctx, mi.act.Head)
				if err != nil {
					log.Error(err)
					return
				}
				if err := mi.state.UnmarshalCBOR(bytes.NewReader(astb)); err != nil {
					log.Error(err)
					return
				}
				mi.info = mi.state.Info
			}

			//
			// Get the Sector Count
			//
			// TODO modfiy api to return ErrNotFound for the case that a miner is not found since that is an expected case..
			/*
				sszs, err := api.StateMinerSectorCount(ctx, k.addr, k.tsKey)
				if err != nil {
					info.psize = 0
					info.ssize = 0
				} else {
					info.psize = sszs.Pset
					info.ssize = sszs.Sset
				}
			*/
			// TODO fix the above code, right now tis slow and fails to find sectors for many miners
		})
		log.Infow("Completed Miner Processing", "duration", time.Since(minerProcessingState).String())

		log.Info("Getting receipts")

		receipts := fetchParentReceipts(ctx, api, toSync)

		log.Info("Storing headers")

		if err := st.storeHeaders(toSync, true); err != nil {
			log.Errorf("%+v", err)
			return
		}

		log.Info("Storing address mapping")

		if err := st.storeAddressMap(addressToID); err != nil {
			log.Error(err)
			return
		}

		log.Info("Storing actors")

		if err := st.storeActors(actors); err != nil {
			log.Error(err)
			return
		}

		log.Info("Storing miners")

		if err := st.storeMiners(miners); err != nil {
			log.Error(err)
			return
		}

		log.Info("Storing miner sectors")

		if err := st.storeSectors(minerTips, api); err != nil {
			log.Error(err)
			return
		}

		log.Infof("Storing messages")

		if err := st.storeMessages(msgs); err != nil {
			log.Error(err)
			return
		}

		log.Info("Storing message inclusions")

		if err := st.storeMsgInclusions(incls); err != nil {
			log.Error(err)
			return
		}

		log.Infof("Storing parent receipts")

		if err := st.storeReceipts(receipts); err != nil {
			log.Error(err)
			return
		}
		log.Infof("Sync stage done")
	}

	log.Infof("Get deals")

	// TODO: incremental, gather expired
	deals, err := api.StateMarketDeals(ctx, headTs.Key())
	if err != nil {
		log.Error(err)
		return
	}

	log.Infof("Store deals")

	if err := st.storeDeals(deals); err != nil {
		log.Error(err)
		return
	}

	log.Infof("Refresh views")

	if err := st.refreshViews(); err != nil {
		log.Error(err)
		return
	}

	log.Infof("Sync done")
}

func fetchMessages(ctx context.Context, api api.FullNode, toSync map[cid.Cid]*types.BlockHeader) (map[cid.Cid]*types.Message, map[cid.Cid][]cid.Cid) {
	var lk sync.Mutex
	messages := map[cid.Cid]*types.Message{}
	inclusions := map[cid.Cid][]cid.Cid{} // block -> msgs

	parmap.Par(50, parmap.MapArr(toSync), func(header *types.BlockHeader) {
		msgs, err := api.ChainGetBlockMessages(ctx, header.Cid())
		if err != nil {
			log.Error(err)
			return
		}

		vmm := make([]*types.Message, 0, len(msgs.Cids))
		for _, m := range msgs.BlsMessages {
			vmm = append(vmm, m)
		}

		for _, m := range msgs.SecpkMessages {
			vmm = append(vmm, &m.Message)
		}

		lk.Lock()
		for _, message := range vmm {
			messages[message.Cid()] = message
			inclusions[header.Cid()] = append(inclusions[header.Cid()], message.Cid())
		}
		lk.Unlock()
	})

	return messages, inclusions
}

type mrec struct {
	msg   cid.Cid
	state cid.Cid
	idx   int
}

func fetchParentReceipts(ctx context.Context, api api.FullNode, toSync map[cid.Cid]*types.BlockHeader) map[mrec]*types.MessageReceipt {
	var lk sync.Mutex
	out := map[mrec]*types.MessageReceipt{}

	parmap.Par(50, parmap.MapArr(toSync), func(header *types.BlockHeader) {
		recs, err := api.ChainGetParentReceipts(ctx, header.Cid())
		if err != nil {
			log.Error(err)
			return
		}
		msgs, err := api.ChainGetParentMessages(ctx, header.Cid())
		if err != nil {
			log.Error(err)
			return
		}

		lk.Lock()
		for i, r := range recs {
			out[mrec{
				msg:   msgs[i].Cid,
				state: header.ParentStateRoot,
				idx:   i,
			}] = r
		}
		lk.Unlock()
	})

	return out
}

// load the power actor state clam as an adt.Map at the tipset `ts`.
func getPowerActorClaimsMap(ctx context.Context, api api.FullNode, ts types.TipSetKey) (*adt.Map, error) {
	powerActor, err := api.StateGetActor(ctx, builtin.StoragePowerActorAddr, ts)
	if err != nil {
		return nil, err
	}

	powerRaw, err := api.ChainReadObj(ctx, powerActor.Head)
	if err != nil {
		return nil, err
	}

	var powerActorState power.State
	if err := powerActorState.UnmarshalCBOR(bytes.NewReader(powerRaw)); err != nil {
		return nil, fmt.Errorf("failed to unmarshal power actor state: %w", err)
	}

	s := &apiIpldStore{ctx, api}
	return adt.AsMap(s, powerActorState.Claims)
}

// require for AMT and HAMT access
// TODO extract this to a common locaiton in lotus and reuse the code
type apiIpldStore struct {
	ctx context.Context
	api api.FullNode
}

func (ht *apiIpldStore) Context() context.Context {
	return ht.ctx
}

func (ht *apiIpldStore) Get(ctx context.Context, c cid.Cid, out interface{}) error {
	raw, err := ht.api.ChainReadObj(ctx, c)
	if err != nil {
		return err
	}

	cu, ok := out.(cbg.CBORUnmarshaler)
	if ok {
		if err := cu.UnmarshalCBOR(bytes.NewReader(raw)); err != nil {
			return err
		}
		return nil
	}

	return fmt.Errorf("Object does not implement CBORUnmarshaler")
}

func (ht *apiIpldStore) Put(ctx context.Context, v interface{}) (cid.Cid, error) {
	return cid.Undef, fmt.Errorf("Put is not implemented on apiIpldStore")
}
