/**
 * Copyright (C) 2017 Cisco Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU General Public License, version 2,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

// @author Changxue Deng <chadeng@cisco.com>

#ifndef __EXPR_PARSER_H__
#define __EXPR_PARSER_H__

#include <string>

// A simple mabain client expression LL parser
class ExprParser
{
public:
    ExprParser(const std::string &expr);
    ~ExprParser();
    int Evaluate(std::string &result);

private:
    int Parse(std::string &result);

    size_t pos;
    std::string expr;
#define EXPR_PARSER_BUFFER_SIZE 1024
    uint8_t buffer[EXPR_PARSER_BUFFER_SIZE];
    std::string err_str;
};

#endif
