/*
    Copyright 2018, Simba Wei.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef EXCEPTIONS_H
#define EXCEPTIONS_H

class IllegalConversionException { };

class IllegalSchemaDeclarationException { };

class UnknownAlgorithmException { };

class UnknownPartitionerException { };

class UnknownHashException { };

class PageFullException {
    public:
        PageFullException(int b) : value(b) { }
        int value;
};

class LoadBZ2Exception { };

class AffinitizationException { };

class FileNotFoundException { };

class ComparisonException { };

class NotYetImplemented { };

#endif // EXCEPTIONS_H
