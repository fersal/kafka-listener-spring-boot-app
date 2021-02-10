package com.fersal.kafka.listener.kafkalistener.model;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface TransactionRepository extends JpaRepository<Transaction, Long> {
    @Query(nativeQuery = true,
            value = "SELECT MAX(transaction_id) FROM transaction_table")
    Long findLastTransaction();
}
