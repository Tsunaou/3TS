/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifdef ENUM_BEGIN
#ifdef ENUM_MEMBER
#ifdef ENUM_END

ENUM_BEGIN(OperationType)
ENUM_MEMBER(OperationType, W)
ENUM_MEMBER(OperationType, R)
ENUM_MEMBER(OperationType, C)
ENUM_MEMBER(OperationType, A)
ENUM_END(OperationType)

ENUM_BEGIN(PreceType)
ENUM_MEMBER(PreceType, RW)
ENUM_MEMBER(PreceType, WR)
ENUM_MEMBER(PreceType, WCR)
ENUM_MEMBER(PreceType, WW)
ENUM_MEMBER(PreceType, WCW)
ENUM_MEMBER(PreceType, RA)
ENUM_MEMBER(PreceType, WC)
ENUM_MEMBER(PreceType, WA)
ENUM_END(PreceType)

ENUM_BEGIN(AnomalyType)
// ======== WAT - 1 =========
ENUM_MEMBER(AnomalyType, WAT_1_DIRTY_WRITE)
ENUM_MEMBER(AnomalyType, WAT_1_FULL_WRITE)
ENUM_MEMBER(AnomalyType, WAT_1_LOST_SELF_UPDATE)
ENUM_MEMBER(AnomalyType, WAT_1_LOST_UPDATE)
// ======== WAT - 2 =========
ENUM_MEMBER(AnomalyType, WAT_2_DOUBLE_WRITE_SKEW_1)
ENUM_MEMBER(AnomalyType, WAT_2_DOUBLE_WRITE_SKEW_2)
ENUM_MEMBER(AnomalyType, WAT_2_READ_WRITE_SKEW_1)
ENUM_MEMBER(AnomalyType, WAT_2_READ_WRITE_SKEW_2)
ENUM_MEMBER(AnomalyType, WAT_2_FULL_WRITE_SKEW)
// ======== WAT - 3 =========
ENUM_MEMBER(AnomalyType, WAT_STEP)
// ======== RAT - 1 =========
ENUM_MEMBER(AnomalyType, RAT_1_DIRTY_READ)
ENUM_MEMBER(AnomalyType, RAT_1_INTERMEDIATE_READ)
ENUM_MEMBER(AnomalyType, RAT_1_NON_REPEATABLE_READ)
// ======== RAT - 2 =========
ENUM_MEMBER(AnomalyType, RAT_2_WRITE_READ_SKEW)
ENUM_MEMBER(AnomalyType, RAT_2_DOUBLE_WRITE_SKEW_COMMITTED)
ENUM_MEMBER(AnomalyType, RAT_2_READ_SKEW)
ENUM_MEMBER(AnomalyType, RAT_2_READ_SKEW_2)
// ======== RAT - 3 =========
ENUM_MEMBER(AnomalyType, RAT_STEP)
// ======== IAT - 1 =========
ENUM_MEMBER(AnomalyType, IAT_1_LOST_UPDATE_COMMITTED)
// ======== IAT - 2 =========
ENUM_MEMBER(AnomalyType, IAT_2_READ_WRITE_SKEW_COMMITTED)
ENUM_MEMBER(AnomalyType, IAT_2_WRITE_SKEW)
// ======== IAT - 3 =========
ENUM_MEMBER(AnomalyType, IAT_STEP)
// ======== Unknown =========
ENUM_MEMBER(AnomalyType, UNKNOWN_1)
ENUM_MEMBER(AnomalyType, UNKNOWN_2)
ENUM_END(AnomalyType)

#endif
#endif
#endif
#ifndef ROW_PRECE_H
#define ROW_PRECE_H

#include <mutex>
#include <unordered_map>
#include "../storage/row.h"

class TxnManager;

#define ENUM_FILE "concurrency_control/row_prece.h"
#include "system/extend_enum.h"

class TxnNode;

inline std::pair<OperationType, OperationType> DividePreceType(const PreceType prece) {
    if (prece == PreceType::WR) {
        return {OperationType::W, OperationType::R};
    } else if (prece == PreceType::WCR) {
        return {OperationType::C, OperationType::R};
    } else if (prece == PreceType::WW) {
        return {OperationType::W, OperationType::W};
    } else if (prece == PreceType::WCW) {
        return {OperationType::C, OperationType::W};
    } else if (prece == PreceType::RW) {
        return {OperationType::R, OperationType::W};
    } else {
        assert(false);
        return {};
    }
}

inline PreceType MergeOperationType(const OperationType o1, const OperationType o2) {
    if (o1 == OperationType::W && o2 == OperationType::R) {
        return PreceType::WR;
    } else if (o1 == OperationType::C && o2 == OperationType::R) {
        return PreceType::WCR;
    } else if (o1 == OperationType::W && o2 == OperationType::W) {
        return PreceType::WW;
    } else if (o1 == OperationType::C && o2 == OperationType::W) {
        return PreceType::WCW;
    } else if (o1 == OperationType::R && o2 == OperationType::W) {
        return PreceType::RW;
    } else {
        assert(false);
        return {};
    }
}

class PreceInfo {
  public:
    PreceInfo(const uint64_t from_txn_id, std::shared_ptr<TxnNode> to_txn, const PreceType type,
            const uint64_t row_id, const uint64_t from_ver_id, const uint64_t to_ver_id)
        : from_txn_id_(from_txn_id), to_txn_(std::move(to_txn)), type_(type), row_id_(row_id),
          from_ver_id_(from_ver_id), to_ver_id_(to_ver_id) {}
    PreceInfo(const PreceInfo&) = default;
    PreceInfo(PreceInfo&&) = default;

    friend std::ostream& operator<<(std::ostream& os, const PreceInfo prece) {
        return os << 'T' << prece.from_txn_id() << "--" << prece.type_ << "(row=" << prece.row_id_ << ")->T" << prece.to_txn_id();
    }

    uint64_t from_txn_id() const { return from_txn_id_; }
    uint64_t to_txn_id() const;
    uint64_t from_ver_id() const { return from_ver_id_; }
    uint64_t to_ver_id() const { return to_ver_id_; }
    OperationType from_op_type() const { return DividePreceType(type_).first; }
    OperationType to_op_type() const { return DividePreceType(type_).second; }
    uint64_t row_id() const { return row_id_; }
    PreceType type() const { return type_; }
    std::shared_ptr<TxnNode> to_txn() const { return to_txn_; }

  private:
    const uint64_t from_txn_id_;
    const std::shared_ptr<TxnNode> to_txn_; // release condition (2) for TxnNode
    const PreceType type_;
    const uint64_t row_id_;
    const uint64_t from_ver_id_;
    const uint64_t to_ver_id_;
};

AnomalyType IdentifyAnomaly(const std::vector<PreceInfo>& preces);

// A TxnNode can be released only when no more transactions build precedence before it. In this case, the
// transaction cannot be a part of cycle anymore.
//
// For latest read, it should satisfies:
// (1) The transaction is already finished (for latest read, no future transactions build RW precedence before
// it).
// (2) There are no transactions built precedence before it.
// We use std::shared_ptr to realize it. When the pointer expired, the two conditions are satisified.
//
// For snapshot read, it should also satisfies:
// (3) Minimum active transaction snapshot timestamp (start timestamp) > this transaction's largest write
// timestamp (commit timestamp). (no future transactions build RW precedence before it)
class TxnNode : public std::enable_shared_from_this<TxnNode>
{
  public:
    TxnNode() : txn_id_() {}

    // we use ver_id but not the count of operation to support snapshot read
    template <PreceType TYPE>
    void AddToTxn(const uint64_t to_txn_id, std::shared_ptr<TxnNode> to_txn_node, const uint64_t row_id,
            const uint64_t from_ver_id, const uint64_t to_ver_id) {
        if (const auto& type = RealPreceType_<TYPE>(); txn_id_ != to_txn_id && type.has_value()) {
            std::lock_guard<std::mutex> l(m_);
            PreceInfo prece = PreceInfo(txn_id_, std::move(to_txn_node), *type, row_id, from_ver_id, to_ver_id);
            // For read/write precedence, only record the first precedence between the two transactions
            to_txns_.try_emplace(to_txn_id, prece);
            // For dirty precedence, W1W2 has higher priority than W1R2 because W1R2C1 is not dirty read
            if ((type == PreceType::WR || type == PreceType::WW) &&
                (!dirty_to_txn_.has_value() || (dirty_to_txn_->type() == PreceType::WR &&
                                                type == PreceType::WW))) {
                dirty_to_txn_.emplace(prece);
            }
        }
    }

    uint64_t txn_id() const { return txn_id_; }
    void set_txn_id(const uint64_t txn_id) { txn_id_ = txn_id; }

    const std::unordered_map<uint64_t, PreceInfo>& UnsafeGetToTxns() const { return to_txns_; }
    const std::optional<PreceInfo>& UnsafeGetDirtyToTxn() const { return dirty_to_txn_; }

    std::mutex& mutex() const { return m_; }

    void commit() {
        std::lock_guard<std::mutex> l(m_);
        is_committed_ = true;
        dirty_to_txn_.reset();
    }

    void abort(bool clear_to_txns) {
        std::lock_guard<std::mutex> l(m_);
        is_committed_ = false;
        dirty_to_txn_.reset();
        if (clear_to_txns) {
            to_txns_.clear();
        }
    }

    bool is_committed() const { return is_committed_.has_value() && *is_committed_; }
    bool is_aborted() const { return is_committed_.has_value() && !(*is_committed_); }
    bool is_active() const { return !is_committed_.has_value(); }

  private:
    std::optional<PreceType> RealPreceType_(const std::optional<PreceType>& active_prece_type,
            const std::optional<PreceType>& committed_prece_type,
            const std::optional<PreceType>& aborted_prece_type) const {
        if (!is_committed_.has_value()) {
            return active_prece_type;
        } else if (is_committed_.value()) {
            return committed_prece_type;
        } else {
            return aborted_prece_type;
        }
    }

    template <PreceType TYPE,
             typename = typename std::enable_if_t<TYPE != PreceType::WCW && TYPE != PreceType::WCR>>
    std::optional<PreceType> RealPreceType_() const {
        if constexpr (TYPE == PreceType::WW) {
            return RealPreceType_(PreceType::WW, PreceType::WCW, {});
        } else if constexpr (TYPE == PreceType::WR) {
            return RealPreceType_(PreceType::WR, PreceType::WCR, {});
        } else {
            return RealPreceType_(TYPE, TYPE, {});
        }
    }

    mutable std::mutex m_;
    uint64_t txn_id_;
    std::optional<bool> is_committed_;
    std::unordered_map<uint64_t, PreceInfo> to_txns_; // key is txn_id
    std::optional<PreceInfo> dirty_to_txn_;
    //std::vector<std::weak_ptr<TxnNode>> from_txns_;
};

// Not thread safe. Protected by RowManager<DLI_IDENTIFY>::m_
//
// A VersionInfo can be released when it will not be read anymore.
//
// For latest read, it should satisfies:
// (1) It is not the latest version.
// (2) The later version's write transaction is finished. (to prevent version revoke)
//
// For snapshot read, it should also satisfies:
// (3) Minimum active transaction snapshot timestamp (start timestamp) > the later version's write timestamp.
class VersionInfo
{
  public:
    VersionInfo(std::weak_ptr<TxnNode> w_txn, row_t* const ver_row, const uint64_t ver_id)
        : w_txn_(std::move(w_txn)), ver_row_(ver_row), ver_id_(ver_id) {}
    VersionInfo(const VersionInfo&) = default;
    VersionInfo(VersionInfo&&) = default;
    ~VersionInfo() {}

    std::shared_ptr<TxnNode> w_txn() const { return w_txn_.lock(); }

    template <typename Task>
    void foreach_r_txn(Task&& task) const {
        for (const auto& r_txn_weak : r_txns_) {
            if (const auto r_txn = r_txn_weak.lock()) {
                task(*r_txn);
            }
        }
    }

    void add_r_txn(std::weak_ptr<TxnNode> txn) { r_txns_.emplace_back(std::move(txn)); }

    row_t* ver_row() const { return ver_row_; }

    uint64_t ver_id() const { return ver_id_; }

  private:
    const std::weak_ptr<TxnNode> w_txn_;
    row_t* const ver_row_;
    // There may be two versions on same the row which have the same ver_id due to version revoking
    const uint64_t ver_id_;
    std::vector<std::weak_ptr<TxnNode>> r_txns_;
};

template <int ALG> class RowManager;

template <>
class RowManager<DLI_IDENTIFY>
{
  public:
    void init(row_t* const orig_row);

    RC read(row_t& ver_row, TxnManager& txn);
    RC prewrite(row_t& ver_row, TxnManager& txn);
    void write(row_t& ver_row, TxnManager& txn);
    void revoke(row_t& ver_row, TxnManager& txn);

  private:
    uint64_t row_id_() const;

    template <PreceType TYPE>
    void build_prece_from_w_txn_(VersionInfo& version, const TxnManager& txn, const uint64_t to_ver_id) const;

    template <PreceType TYPE>
    void build_preces_from_r_txns_(VersionInfo& version, const TxnManager& txn,
            const uint64_t to_ver_id) const;

    template <PreceType TYPE>
    void build_prece(TxnNode& from_txn, const TxnManager& txn, const uint64_t from_ver_id,
            const uint64_t to_ver_id) const;

  private:
    row_t* orig_row_;
    std::mutex m_;
    //std::map<uint64_t, std::shared_ptr<VersionInfo>> versions_; // key is write timestamp (snapshot read)
    std::shared_ptr<VersionInfo> latest_version_;
};

#endif