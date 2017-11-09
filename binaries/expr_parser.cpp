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

#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <iostream>

#include "expr_parser.h"
#include "hexbin.h"

ExprParser::ExprParser(const std::string &exp)
{
    pos = 0;
    expr = exp;
    err_str = "";
}

ExprParser::~ExprParser()
{
}

int ExprParser::Evaluate(std::string &result)
{
    result = "";
    int rval = Parse(result);
    if(rval < 0)
    {
        std::cout << err_str << "\n";
    }
    else if(rval == 0 && pos < expr.length())
    {
        std::cout << "extra string " + expr.substr(pos)
                  << " found at the end of expression\n";
        return -1;
    }

    return rval;
}

// Grammer:
//    S ---> E
//    E ---> bin( E )
//    E ---> hex( E )
//    E ---> T
//    E ---> T + E (NOT IMPLEMENTED YET)
//    T ---> literal string
// Sample expressions
// literal string: 'abcdefg'
// convert hex to binary: bin('0001ff01')
// convert hex to binary and back to hex:
// hex(bin('4da32feebf1611e7bd98c0b13cdb778c'))=='4da32feebf1611e7bd98c0b13cdb778c'
int ExprParser::Parse(std::string &result)
{
    if(pos >= expr.length())
        return 0;

    int rval;
    switch(expr[pos])
    {
        case 'b':
            if(expr.find("in(", pos+1, 3) == std::string::npos)
            {
                err_str = "unrecognized expression " + expr.substr(pos);
                return -1;
            }
            pos += 4;
            rval = Parse(result);
            if(rval < 0)
                return rval;
            if(expr[pos] != ')')
            {
                err_str = "missing ) at the end of " + expr.substr(0, pos);
                return -1;
            }
            pos++;
            rval = hex_2_bin(result.c_str(), result.length(), buffer,
                             EXPR_PARSER_BUFFER_SIZE);
            if(rval < 0)
            {
                err_str = "failed to convert hex string " + result + " to binary format";
                return -1;
            }
            result = std::string((char *)buffer, rval);
            break;
        case 'h':
            if(expr.find("ex(", pos+1, 3) == std::string::npos)
            {
                err_str = "unrecognized expression " + expr.substr(pos);
                return -1;
            }
            pos += 4;
            rval = Parse(result);
            if(rval < 0)
                return rval;
            if(expr[pos] != ')')
            {
                err_str = "missing ) at the end of " + expr.substr(0, pos);
                return -1;
            }
            pos++;
            rval = bin_2_hex((const uint8_t *)result.data(), result.size(),
                             (char *)buffer, EXPR_PARSER_BUFFER_SIZE);
            if(rval < 0)
            {
                err_str = "failed to convert binary string to hex";
                return -1;
            }
            result = std::string((char *)buffer, rval);
            break;
        case '\'':
            {
                size_t endp = expr.find('\'', pos+1);
                if(endp ==  std::string::npos)
                {
                    err_str = "expression " + expr.substr(0, pos) + " missing closing \'";
                    return -1;
                }
                result = expr.substr(pos+1, endp-pos-1);
                pos = endp + 1;
            }
            break;
        default:
            err_str  = "unrecognized expression " + expr.substr(pos) + "\n";
            err_str += "                        ^";
            return -1; 
    }

    return 0;
}
