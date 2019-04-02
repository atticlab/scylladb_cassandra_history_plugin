#pragma once
#include <cassandra.h>

#include <functional>
#include <iostream>


class exit_scope
{
public:
	exit_scope( const std::function<void(void)>& callback ):callback_(callback){}
	~exit_scope(){ if (callback_) callback_(); }

    void reset() { callback_ = std::function<void()>(); }
private:
	exit_scope();
	exit_scope(const exit_scope&);
	exit_scope& operator =(const exit_scope&);
	std::function<void(void)> callback_;
};


template <typename T, typename ReleaseFunctionType>
class CassandraPointerGuard
{
public:
    CassandraPointerGuard(T* p, ReleaseFunctionType* f)
        : raw_(p), f_release_(f)
    {
    }

    CassandraPointerGuard(CassandraPointerGuard&& other)
    {
        raw_ = other.raw_;
        f_release_ = other.f_release_;

        other.raw_ = nullptr;
        other.f_release_ = nullptr;
    }

    ~CassandraPointerGuard()
    {
        if (raw_)
        {
            f_release_(raw_);
        }
    }

    CassandraPointerGuard& operator=(CassandraPointerGuard&& other)
    {
        if (this == &other)
        {
            return *this;
        }

        if (raw_)
        {
            f_release_(raw_);
        }

        raw_ = other.raw_;
        f_release_ = other.f_release_;

        other.raw_ = nullptr;
        other.f_release_ = nullptr;
        return *this;
    }

    auto get() const { return raw_; }
    void drop() { raw_ = nullptr; }
    void reset(T* p = nullptr)
    {
        if (raw_ == p)
        {
            return;
        }

        if (raw_)
        {
            f_release_(raw_);
        }
        raw_ = p;
    }
    void swap(CassandraPointerGuard& other) { std::swap(raw_, other.raw_); }

private:
    CassandraPointerGuard(const CassandraPointerGuard& other) = delete;
    CassandraPointerGuard& operator=(const CassandraPointerGuard& other) = delete;

    T* raw_;
    ReleaseFunctionType* f_release_;
};


using cluster_guard   = CassandraPointerGuard<CassCluster,         decltype(cass_cluster_free)>;
using session_guard   = CassandraPointerGuard<CassSession,         decltype(cass_session_free)>;
using future_guard    = CassandraPointerGuard<CassFuture,          decltype(cass_future_free)>;
using statement_guard = CassandraPointerGuard<CassStatement,       decltype(cass_statement_free)>;
using batch_guard     = CassandraPointerGuard<CassBatch,           decltype(cass_batch_free)>;
using prepared_guard  = CassandraPointerGuard<const CassPrepared,  decltype(cass_prepared_free)>;