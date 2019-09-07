package com.batch.mapper;

import com.batch.model.Bill;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RecordRowMapper implements RowMapper<Bill> {
    @Override
    public Bill mapRow(ResultSet rs, int rowNum) throws SQLException {
        Bill bill = new Bill();
        bill.setId(rs.getLong("id"));
        bill.setFirstName(rs.getString("first_name"));
        bill.setLastName(rs.getString("last_name"));
        bill.setDataUsage(rs.getLong("data_usage"));
        bill.setMinutes(rs.getLong("minutes"));
        bill.setBillAmount(rs.getDouble("bill_amount"));
        return bill;
    }
}
