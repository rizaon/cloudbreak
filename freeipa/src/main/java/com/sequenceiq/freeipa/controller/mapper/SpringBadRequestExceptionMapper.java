package com.sequenceiq.freeipa.controller.mapper;

import javax.ws.rs.core.Response.Status;

import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.common.exception.BadRequestException;
import com.sequenceiq.cloudbreak.exception.mapper.BaseExceptionMapper;

@Component
public class SpringBadRequestExceptionMapper extends BaseExceptionMapper<BadRequestException> {

    @Override
    public Status getResponseStatus(BadRequestException exception) {
        return Status.BAD_REQUEST;
    }

    @Override
    public Class<BadRequestException> getExceptionType() {
        return BadRequestException.class;
    }

}