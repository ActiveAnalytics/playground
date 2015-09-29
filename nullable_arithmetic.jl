# Basic ideas around implementing nullable arithmetic in Julia

# Convert Nullables
import Base: convert

# Conversion
function convert{T, U}(x::Type{Nullable{T}}, y::U)
  Nullable{T}(convert(T, y))
end

# Reverse conversion
function convert{T, U}(x::Type{U}, y::Nullable{T})
  convert(U, y.value)
end

# Promote Nullable, U
function promote{T <: Nullable, U}(x::T, y::U)
  X = promote_type(T, U)
  return (Nullable{X}(x.value), Nullable{X}(y))
end

# Promote U, Nullable
function promote{U, T <: Nullable}(x::U, y::T)
  X = promote_type(U, T)
  return (Nullable{X}(x), Nullable{X}(y.value))
end


# Import base binary operators
import Base: +, -, *, /, \, ^, %, &, |, $, >>>, >>, <<, ==, !=, <, <=, >, >=

# Define binary operators
arith = [:+, :-, :*, :/, :\, :^, :%]
bit = [:&, :|, :$, :>>>, :>>, :<<]
update = [:+=, :-=, :/=, :\=, :%=, :^=, :|=, :>>>=, :>>=, :<<=]
comp = [:(==), :!=, :<, :<=, :>, :>=]

# Combine to one vector
syms = [arith; bit; update; comp]

# Symbol expression union type
sym_expr = Union{Symbol, Expr}

# Nullable{T} and Nullable{T} family
function exnn{Q<:Union{Symbol, Expr}}(sym::Q)
  quote
    function $sym{T<:Nullable, U<:Nullable}(x::T, y::U)
    X = promote_type(eltype(x), eltype(y))
      try
        Nullable{X}($sym(x.value, y.value))
      catch
        Nullable{X}()
      end
    end
  end
end

# For the Nullable{T} and U family
function exnu{Q<:Union{Symbol, Expr}}(sym::Q)
  quote
    function $sym{T<:Nullable, U}(x::T, y::U)
      z = promote(x, y)
      $sym(z[1], z[2])
    end
  end
end

# For the U and Nullable{T} family
function exun{Q<:Union{Symbol, Expr}}(sym::Q)
  quote
    function $sym{U, T<:Nullable}(x::U, y::T)
      z = promote(x, y)
      $sym(z[1], z[2])
    end
  end
end


# Source the functions
for i in [exnn, exnu, exun]
  for j in syms
    eval(i(j))
  end
end
