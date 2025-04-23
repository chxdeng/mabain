/**
 * Copyright (C) 2025 Cisco Inc.
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

#include "expr_parser.h"
#include "hexbin.h"
#include <cctype>
#include <iostream>

// Grammar:
//    S  ---> E
//    E  ---> bin(E)
//    E  ---> hex(E)
//    E  ---> T
//    E  ---> T E     (concatenation, left-to-right)
//    T  ---> literal string ("abc", 'xyz')
//    T  ---> hex literal (e.g., 0x1234 inside bin())
//
// Sample expressions:
//   "abcdefg"
//   bin("0001ff01")
//   bin(0xdeadbeef)
//   "abc"bin(0x0001)"def"

ExprParser::ExprParser(const std::string& exp)
{
    pos = 0;
    expr = exp;
    err_str = "";
}

ExprParser::~ExprParser()
{
}

int ExprParser::Evaluate(std::string& result)
{
    result = "";
    int rval = Parse(result);
    if (rval < 0) {
        std::cout << err_str << "\n";
    } else if (rval == 0 && pos < expr.length()) {
        std::cout << "extra string " + expr.substr(pos)
                  << " found at the end of expression\n";
        return -1;
    }

    return rval;
}

int ExprParser::Parse(std::string& result)
{
    result.clear(); // Final result is built by appending parsed sub-results

    while (pos < expr.length()) {
        std::string sub_result;
        int rval;

        switch (expr[pos]) {
        case 'b': { // bin(...) expression
            if (expr.find("in(", pos + 1, 3) == std::string::npos) {
                err_str = "unrecognized expression " + expr.substr(pos);
                return -1;
            }

            pos += 4; // skip "bin("
            rval = Parse(sub_result); // parse inner expression
            if (rval < 0)
                return rval;

            if (pos >= expr.length() || expr[pos] != ')') {
                err_str = "missing ) at the end of " + expr.substr(0, pos);
                return -1;
            }
            pos++; // skip ')'

            std::string hex_input = sub_result;
            if (hex_input.rfind("0x", 0) == 0 || hex_input.rfind("0X", 0) == 0) {
                hex_input = hex_input.substr(2);
            }

            rval = hex_2_bin(hex_input.c_str(), hex_input.length(), buffer,
                EXPR_PARSER_BUFFER_SIZE);
            if (rval < 0) {
                err_str = "failed to convert hex string " + sub_result + " to binary format";
                return -1;
            }

            sub_result = std::string((char*)buffer, rval);
            break;
        }
        case 'h': { // hex(...) expression
            if (expr.find("ex(", pos + 1, 3) == std::string::npos) {
                err_str = "unrecognized expression " + expr.substr(pos);
                return -1;
            }

            pos += 4; // skip "hex("
            rval = Parse(sub_result);
            if (rval < 0)
                return rval;

            if (pos >= expr.length() || expr[pos] != ')') {
                err_str = "missing ) at the end of " + expr.substr(0, pos);
                return -1;
            }
            pos++; // skip ')'

            rval = bin_2_hex((const uint8_t*)sub_result.data(), sub_result.size(),
                (char*)buffer, EXPR_PARSER_BUFFER_SIZE);
            if (rval < 0) {
                err_str = "failed to convert binary string to hex";
                return -1;
            }

            sub_result = std::string((char*)buffer, rval);
            break;
        }
        case '"': { // Literal string: "abc"
            size_t endp = expr.find('"', pos + 1);
            if (endp == std::string::npos) {
                err_str = "expression " + expr.substr(0, pos) + " missing closing \"";
                return -1;
            }

            sub_result = expr.substr(pos + 1, endp - pos - 1);
            pos = endp + 1;
            break;
        }
        case '\'': { // Literal string: 'abc'
            size_t endp = expr.find('\'', pos + 1);
            if (endp == std::string::npos) {
                err_str = "expression " + expr.substr(0, pos) + " missing closing \'";
                return -1;
            }

            sub_result = expr.substr(pos + 1, endp - pos - 1);
            pos = endp + 1;
            break;
        }
        case '0': { // Possibly 0x... hex string
            size_t start = pos;
            if ((expr.length() > pos + 1) && (expr[pos + 1] == 'x' || expr[pos + 1] == 'X')) {
                pos += 2;
                size_t hex_start = pos;
                while (pos < expr.length() && isxdigit(expr[pos])) {
                    pos++;
                }
                if (hex_start == pos) {
                    err_str = "expected hex digits after 0x";
                    return -1;
                }
                sub_result = expr.substr(start, pos - start); // includes 0x prefix
                break;
            }
            // If not 0x, fall through to default error
            [[fallthrough]];
        }
        case ')': // End of parenthesized expression
            return 0;
        default: { // Unknown or unsupported character
            err_str = "unrecognized expression " + expr.substr(pos) + "\n";
            err_str += "                        ^";
            return -1;
        }
        }

        // Append parsed component to final result
        result += sub_result;
    }

    return 0;
}
